# frozen_string_literal: true

require 'json'
require 'time'

module MessageDeliverTest
  class RedisStatisticsStore
    include MessageDeliverTest::Constants

    def initialize(run_identifier)
      @run_identifier = run_identifier.to_s
      raise ArgumentError, 'run_identifier is blank' if @run_identifier.empty?
    end

    def initialize_run!(
      messages_per_user:,
      table_count:,
      sender_user_id:,
      expected_job_count:,
      database_ids:
    )
      with_redis do |redis_connection|
        redis_connection.multi do
          redis_connection.del(
            run_metadata_key,
            completed_job_count_key,
            finalize_execution_lock_key,
            run_database_ids_key,
            run_error_events_key
          )

          redis_connection.hmset(
            run_metadata_key,
            REDIS_FIELD_RUN_ID, @run_identifier,
            REDIS_FIELD_RUN_STATUS, 'running',
            REDIS_FIELD_STARTED_AT, Time.now.utc.iso8601,
            REDIS_FIELD_MESSAGES_PER_USER, messages_per_user.to_i,
            REDIS_FIELD_TABLE_COUNT, table_count.to_i,
            REDIS_FIELD_SENDER_USER_ID, sender_user_id.to_i,
            REDIS_FIELD_EXPECTED_JOB_COUNT, expected_job_count.to_i,
            REDIS_FIELD_FAILED_JOB_COUNT, 0
          )

          redis_connection.set(completed_job_count_key, 0)
          database_ids.each { |database_id| redis_connection.sadd(run_database_ids_key, database_id.to_i) }

          set_expiration_for_run_keys(redis_connection, [run_metadata_key, completed_job_count_key, run_database_ids_key, run_error_events_key])
        end
      end
    end

    def store_database_metadata!(database_id:, database_host:, database_name:)
      database_statistics_hash_key = database_statistics_key(database_id)

      with_redis do |redis_connection|
        redis_connection.multi do
          redis_connection.sadd(run_database_ids_key, database_id.to_i)
          redis_connection.hmset(
            database_statistics_hash_key,
            REDIS_FIELD_DATABASE_HOST, database_host.to_s,
            REDIS_FIELD_DATABASE_NAME, database_name.to_s
          )

          set_expiration_for_run_keys(redis_connection, [run_database_ids_key, database_statistics_hash_key, database_histogram_key(database_id)])
        end
      end
    end

    def record_successful_job_once!(
      database_id:,
      table_id:,
      user_creation_duration_seconds:,
      message_delivery_total_duration_seconds:,
      delivered_messages_count:,
      delivery_errors_count:,
      message_time_sum_ms:,
      message_time_sum_of_squares_ms:,
      message_time_min_ms:,
      message_time_max_ms:,
      histogram_bucket_counts:
    )
      job_state_tracking_key = job_accounting_state_key(database_id, table_id)
      database_statistics_hash_key = database_statistics_key(database_id)
      database_histogram_hash_key = database_histogram_key(database_id)

      watch_attempt_index = 0
      while watch_attempt_index < REDIS_WATCH_RETRY_LIMIT
        transaction_outcome = with_redis do |redis_connection|
          redis_connection.watch(job_state_tracking_key, database_statistics_hash_key, completed_job_count_key)

          existing_job_state = redis_connection.get(job_state_tracking_key)
          if existing_job_state
            redis_connection.unwatch
            :already_recorded
          else
            current_min_message_time_ms = to_float_or_nil(redis_connection.hget(database_statistics_hash_key, REDIS_FIELD_MESSAGE_TIME_MIN_MS))
            current_max_message_time_ms = to_float_or_nil(redis_connection.hget(database_statistics_hash_key, REDIS_FIELD_MESSAGE_TIME_MAX_MS))

            next_min_message_time_ms = choose_smaller_float(current_min_message_time_ms, message_time_min_ms)
            next_max_message_time_ms = choose_larger_float(current_max_message_time_ms, message_time_max_ms)

            redis_transaction_response = redis_connection.multi do
              redis_connection.set(job_state_tracking_key, JOB_ACCOUNTING_STATE_SUCCESS)
              redis_connection.expire(job_state_tracking_key, RUN_DATA_TTL_SECONDS)

              redis_connection.hincrby(database_statistics_hash_key, REDIS_FIELD_USERS_TOTAL, 1)
              redis_connection.hincrby(database_statistics_hash_key, REDIS_FIELD_USERS_WITH_SENT_MESSAGES, 1) if delivered_messages_count.to_i > 0
              redis_connection.hincrbyfloat(database_statistics_hash_key, REDIS_FIELD_USER_CREATION_TIME_SUM_SECONDS, user_creation_duration_seconds.to_f)
              redis_connection.hincrbyfloat(database_statistics_hash_key, REDIS_FIELD_MESSAGE_DELIVERY_TIME_SUM_SECONDS, message_delivery_total_duration_seconds.to_f)
              redis_connection.hincrby(database_statistics_hash_key, REDIS_FIELD_TOTAL_MESSAGES_SENT, delivered_messages_count.to_i)
              redis_connection.hincrby(database_statistics_hash_key, REDIS_FIELD_TOTAL_MESSAGE_SEND_ERRORS, delivery_errors_count.to_i)
              redis_connection.hincrbyfloat(database_statistics_hash_key, REDIS_FIELD_MESSAGE_TIME_SUM_MS, message_time_sum_ms.to_f)
              redis_connection.hincrbyfloat(database_statistics_hash_key, REDIS_FIELD_MESSAGE_TIME_SUM_OF_SQUARES_MS, message_time_sum_of_squares_ms.to_f)

              redis_connection.hset(database_statistics_hash_key, REDIS_FIELD_MESSAGE_TIME_MIN_MS, next_min_message_time_ms) unless next_min_message_time_ms.nil?
              redis_connection.hset(database_statistics_hash_key, REDIS_FIELD_MESSAGE_TIME_MAX_MS, next_max_message_time_ms) unless next_max_message_time_ms.nil?

              histogram_bucket_counts.each do |bucket_index, bucket_count|
                redis_connection.hincrby(database_histogram_hash_key, bucket_index.to_i, bucket_count.to_i) if bucket_count.to_i > 0
              end

              redis_connection.incr(completed_job_count_key)

              set_expiration_for_run_keys(
                redis_connection,
                [database_statistics_hash_key, database_histogram_hash_key, completed_job_count_key]
              )
            end

            redis_transaction_response.nil? ? :transaction_conflict : :recorded
          end
        end

        return false if transaction_outcome == :already_recorded
        return true if transaction_outcome == :recorded

        watch_attempt_index += 1
      end

      false
    end

    def record_failed_job_once!(database_id:, table_id:, failed_stage:, failed_error_class:, failed_error_message:)
      job_state_tracking_key = job_accounting_state_key(database_id, table_id)
      database_statistics_hash_key = database_statistics_key(database_id)

      watch_attempt_index = 0
      while watch_attempt_index < REDIS_WATCH_RETRY_LIMIT
        transaction_outcome = with_redis do |redis_connection|
          redis_connection.watch(job_state_tracking_key, run_metadata_key, completed_job_count_key)

          existing_job_state = redis_connection.get(job_state_tracking_key)
          if existing_job_state
            redis_connection.unwatch
            :already_recorded
          else
            error_event_payload = {
              'run_id' => @run_identifier,
              'database_id' => database_id.to_i,
              'table_id' => table_id.to_i,
              'failed_stage' => failed_stage.to_s,
              'error_class' => failed_error_class.to_s,
              'error_message' => failed_error_message.to_s,
              'recorded_at' => Time.now.utc.iso8601
            }

            redis_transaction_response = redis_connection.multi do
              redis_connection.set(job_state_tracking_key, JOB_ACCOUNTING_STATE_FAILED)
              redis_connection.expire(job_state_tracking_key, RUN_DATA_TTL_SECONDS)

              redis_connection.hincrby(run_metadata_key, REDIS_FIELD_FAILED_JOB_COUNT, 1)
              redis_connection.hincrby(database_statistics_hash_key, REDIS_FIELD_TOTAL_MESSAGE_SEND_ERRORS, 1)
              redis_connection.incr(completed_job_count_key)

              redis_connection.rpush(run_error_events_key, JSON.generate(error_event_payload))
              redis_connection.ltrim(run_error_events_key, -STORED_ERROR_EVENTS_LIMIT, -1)

              set_expiration_for_run_keys(redis_connection, [run_metadata_key, database_statistics_hash_key, completed_job_count_key, run_error_events_key])
            end

            redis_transaction_response.nil? ? :transaction_conflict : :recorded
          end
        end

        return false if transaction_outcome == :already_recorded
        return true if transaction_outcome == :recorded

        watch_attempt_index += 1
      end

      false
    end

    def expected_job_count
      run_metadata[REDIS_FIELD_EXPECTED_JOB_COUNT].to_i
    end

    def completed_job_count
      with_redis { |redis_connection| redis_connection.get(completed_job_count_key).to_i }
    end

    def all_jobs_accounted_for?
      expected_job_count.positive? && completed_job_count >= expected_job_count
    end

    def acquire_finalize_execution_lock!
      lock_was_set = with_redis { |redis_connection| redis_connection.setnx(finalize_execution_lock_key, Time.now.utc.iso8601) }
      lock_was_acquired = (lock_was_set == true || lock_was_set == 1)
      with_redis { |redis_connection| redis_connection.expire(finalize_execution_lock_key, RUN_DATA_TTL_SECONDS) } if lock_was_acquired
      lock_was_acquired
    end

    def run_metadata
      with_redis { |redis_connection| redis_connection.hgetall(run_metadata_key) || {} }
    end

    def database_ids
      with_redis { |redis_connection| (redis_connection.smembers(run_database_ids_key) || []).map(&:to_i).sort }
    end

    def database_statistics(database_id)
      with_redis { |redis_connection| redis_connection.hgetall(database_statistics_key(database_id)) || {} }
    end

    def database_histogram(database_id)
      with_redis { |redis_connection| redis_connection.hgetall(database_histogram_key(database_id)) || {} }
    end

    def error_events(limit:)
      upper_bound_index = [limit.to_i - 1, 0].max
      rows = with_redis { |redis_connection| redis_connection.lrange(run_error_events_key, 0, upper_bound_index) || [] }
      rows.map { |row| JSON.parse(row) }
    end

    def error_event_count
      with_redis { |redis_connection| redis_connection.llen(run_error_events_key).to_i }
    end

    def mark_run_as_finished!(report_file_path:)
      with_redis do |redis_connection|
        redis_connection.multi do
          redis_connection.hmset(
            run_metadata_key,
            REDIS_FIELD_RUN_STATUS, 'finished',
            REDIS_FIELD_FINISHED_AT, Time.now.utc.iso8601,
            REDIS_FIELD_REPORT_PATH, report_file_path.to_s
          )
          set_expiration_for_run_keys(redis_connection, [run_metadata_key, completed_job_count_key, finalize_execution_lock_key, run_database_ids_key, run_error_events_key])
        end
      end
    end

    def run_status_snapshot
      metadata = run_metadata
      metadata['completed_job_count'] = completed_job_count
      metadata['error_event_count'] = error_event_count
      metadata
    end

    private

    def with_redis(&block)
      Sidekiq.redis(&block)
    end

    def set_expiration_for_run_keys(redis_connection, keys)
      keys.each { |key| redis_connection.expire(key, RUN_DATA_TTL_SECONDS) }
    end

    def choose_smaller_float(current_value, candidate_value)
      return current_value if candidate_value.nil?
      return candidate_value if current_value.nil?
      candidate_value < current_value ? candidate_value : current_value
    end

    def choose_larger_float(current_value, candidate_value)
      return current_value if candidate_value.nil?
      return candidate_value if current_value.nil?
      candidate_value > current_value ? candidate_value : current_value
    end

    def to_float_or_nil(value)
      return nil if value.nil?
      value.to_f
    end

    def base_run_key_prefix
      "message_deliver_test:#{@run_identifier}"
    end

    def run_metadata_key
      "#{base_run_key_prefix}:run_metadata"
    end

    def completed_job_count_key
      "#{base_run_key_prefix}:completed_job_count"
    end

    def finalize_execution_lock_key
      "#{base_run_key_prefix}:finalize_execution_lock"
    end

    def run_database_ids_key
      "#{base_run_key_prefix}:database_ids"
    end

    def run_error_events_key
      "#{base_run_key_prefix}:error_events"
    end

    def database_statistics_key(database_id)
      "#{base_run_key_prefix}:database:#{database_id.to_i}:statistics"
    end

    def database_histogram_key(database_id)
      "#{base_run_key_prefix}:database:#{database_id.to_i}:message_time_histogram"
    end

    def job_accounting_state_key(database_id, table_id)
      "#{base_run_key_prefix}:job_accounting:database:#{database_id.to_i}:table:#{table_id.to_i}:state"
    end
  end
end
