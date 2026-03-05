# frozen_string_literal: true

require 'securerandom'

module Jobs
  class SendMessageDeliverTestJob
    include Sidekiq::Worker
    include MessageDeliverTest::Constants

    MONOTONIC_CLOCK_SOURCE = Process::CLOCK_MONOTONIC
    ZERO_DELIVERY_ERROR_COUNT = 0

    sidekiq_options queue: DEFAULT_SIDEKIQ_QUEUE, retry: WORKER_JOB_SIDEKIQ_RETRY_COUNT

    sidekiq_retries_exhausted do |job_payload, raised_error|
      run_identifier, database_id, table_id = Array(job_payload['args'])
      normalized_run_identifier = run_identifier.to_s
      next if normalized_run_identifier.empty?

      error_class_name = raised_error ? raised_error.class.name : 'UnknownSidekiqError'
      error_message = raised_error ? raised_error.message : 'Sidekiq retries exhausted'

      redis_store = MessageDeliverTest::RedisStatisticsStore.new(normalized_run_identifier)

      redis_store.record_failed_job_once!(
        database_id: database_id,
        table_id: table_id,
        failed_stage: 'job_retries_exhausted',
        failed_error_class: error_class_name,
        failed_error_message: error_message
      )

      if redis_store.all_jobs_accounted_for? && redis_store.acquire_finalize_execution_lock!
        Jobs::FinalizeMessageDeliverTestJob.perform_async(normalized_run_identifier)
      end
    end

    def perform(run_identifier, database_id, table_id, messages_per_user = DEFAULT_MESSAGES_PER_USER, sender_user_id = DEFAULT_SENDER_USER_ID)
      normalized_run_identifier = run_identifier.to_s
      normalized_database_id = database_id.to_i
      normalized_table_id = table_id.to_i
      normalized_messages_per_user = messages_per_user.to_i
      normalized_sender_user_id = sender_user_id.to_i

      redis_store = MessageDeliverTest::RedisStatisticsStore.new(normalized_run_identifier)

      sender_user = User.abstract_find(normalized_sender_user_id)

      user_creation_started_at = monotonic_time_now
      target_user = create_target_user_for_test!
      attach_user_to_messages_shard!(
        user_id: target_user.id,
        database_id: normalized_database_id,
        table_id: normalized_table_id
      )
      user_creation_duration_seconds = monotonic_time_now - user_creation_started_at

      message_delivery_statistics = deliver_messages_and_collect_statistics!(
        sender_user: sender_user,
        target_user_id: target_user.id,
        messages_per_user: normalized_messages_per_user
      )

      redis_store.record_successful_job_once!(
        database_id: normalized_database_id,
        table_id: normalized_table_id,
        user_creation_duration_seconds: user_creation_duration_seconds,
        message_delivery_total_duration_seconds: message_delivery_statistics[:message_delivery_total_duration_seconds],
        delivered_messages_count: message_delivery_statistics[:delivered_messages_count],
        delivery_errors_count: ZERO_DELIVERY_ERROR_COUNT,
        message_time_sum_ms: message_delivery_statistics[:message_time_sum_ms],
        message_time_sum_of_squares_ms: message_delivery_statistics[:message_time_sum_of_squares_ms],
        message_time_min_ms: message_delivery_statistics[:message_time_min_ms],
        message_time_max_ms: message_delivery_statistics[:message_time_max_ms],
        histogram_bucket_counts: message_delivery_statistics[:histogram_bucket_counts]
      )

      if redis_store.all_jobs_accounted_for? && redis_store.acquire_finalize_execution_lock!
        Jobs::FinalizeMessageDeliverTestJob.perform_async(normalized_run_identifier)
      end
    end

    private

    def deliver_messages_and_collect_statistics!(sender_user:, target_user_id:, messages_per_user:)
      message_delivery_started_at = monotonic_time_now
      delivered_messages_count = 0
      message_time_sum_ms = 0.0
      message_time_sum_of_squares_ms = 0.0
      message_time_min_ms = nil
      message_time_max_ms = nil
      histogram_bucket_counts = {}

      messages_per_user.times do |message_index|
        single_message_started_at = monotonic_time_now
        MessageWorkflow
          .new(sender_user)
          .create_message_for_deliver(
            "Load test message ##{message_index} for user ##{target_user_id}",
            target_user_id,
            :web
          )
        single_message_duration_ms = (monotonic_time_now - single_message_started_at) * MILLISECONDS_IN_SECOND

        delivered_messages_count += 1
        message_time_sum_ms += single_message_duration_ms
        message_time_sum_of_squares_ms += (single_message_duration_ms * single_message_duration_ms)
        message_time_min_ms = choose_smaller_value(message_time_min_ms, single_message_duration_ms)
        message_time_max_ms = choose_larger_value(message_time_max_ms, single_message_duration_ms)

        histogram_bucket_index = histogram_bucket_index_for(single_message_duration_ms)
        histogram_bucket_counts[histogram_bucket_index] = histogram_bucket_counts.fetch(histogram_bucket_index, 0) + 1
      end

      {
        delivered_messages_count: delivered_messages_count,
        message_time_sum_ms: message_time_sum_ms,
        message_time_sum_of_squares_ms: message_time_sum_of_squares_ms,
        message_time_min_ms: message_time_min_ms,
        message_time_max_ms: message_time_max_ms,
        histogram_bucket_counts: histogram_bucket_counts,
        message_delivery_total_duration_seconds: monotonic_time_now - message_delivery_started_at
      }
    end

    def create_target_user_for_test!
      User.on_table.create!(
        login: "user#{SecureRandom.hex.chars.sample(12).join}",
        password: SecureRandom.hex[0..16],
        profile_attributes: {
          'birthdate(3i)' => Date.today.day.to_s,
          'birthdate(2i)' => Date.today.month.to_s,
          'birthdate(1i)' => (Date.today.year - TEST_USER_AGE_YEARS).to_s,
          username: 'TestMessageShard',
          sex: TEST_USER_SEX,
          country_id: TEST_USER_COUNTRY_ID,
          city_id: TEST_USER_CITY_ID
        }
      )
    end

    def attach_user_to_messages_shard!(user_id:, database_id:, table_id:)
      messages_shard_link = MessagesShard.using(:shard_one).new
      messages_shard_link.user_id = user_id
      messages_shard_link.db_id = database_id
      messages_shard_link.table_id = table_id
      messages_shard_link.save!
    end

    def histogram_bucket_index_for(message_duration_ms)
      bucket_index = (message_duration_ms / MESSAGE_TIME_HISTOGRAM_BUCKET_SIZE_MS).floor
      return 0 if bucket_index < 0
      return MESSAGE_TIME_HISTOGRAM_MAX_BUCKET_INDEX if bucket_index > MESSAGE_TIME_HISTOGRAM_MAX_BUCKET_INDEX

      bucket_index
    end

    def choose_smaller_value(current_value, candidate_value)
      return candidate_value if current_value.nil?
      candidate_value < current_value ? candidate_value : current_value
    end

    def choose_larger_value(current_value, candidate_value)
      return candidate_value if current_value.nil?
      candidate_value > current_value ? candidate_value : current_value
    end

    def monotonic_time_now
      Process.clock_gettime(MONOTONIC_CLOCK_SOURCE)
    end
  end
end
