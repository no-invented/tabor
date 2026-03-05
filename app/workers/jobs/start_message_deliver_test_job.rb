# frozen_string_literal: true

require 'securerandom'
require 'sidekiq/client'

module Jobs
  class StartMessageDeliverTestJob
    include Sidekiq::Worker
    include MessageDeliverTest::Constants

    sidekiq_options queue: DEFAULT_SIDEKIQ_QUEUE, retry: START_JOB_SIDEKIQ_RETRY_COUNT

    def self.enqueue!(options = {})
      normalized_options = stringify_hash_keys(options || {})
      normalized_options['run_identifier'] = generated_run_identifier if normalized_options['run_identifier'].to_s.empty?
      perform_async(normalized_options)
      normalized_options['run_identifier']
    end

    def perform(options = {})
      normalized_options = self.class.stringify_hash_keys(options || {})

      run_identifier = normalized_options.fetch('run_identifier').to_s
      raise ArgumentError, 'run_identifier is blank' if run_identifier.empty?

      messages_per_user = parse_positive_integer(
        normalized_options['messages_per_user'],
        DEFAULT_MESSAGES_PER_USER
      )
      sender_user_id = parse_positive_integer(
        normalized_options['sender_user_id'],
        DEFAULT_SENDER_USER_ID
      )
      table_count = parse_positive_integer(
        normalized_options['table_count'],
        configured_table_count
      )
      explicit_database_ids = parse_database_ids(normalized_options['database_ids'])
      selected_database_ids = explicit_database_ids.empty? ? default_database_ids : explicit_database_ids

      User.abstract_find(sender_user_id)

      expected_job_count = selected_database_ids.length * table_count
      raise ArgumentError, 'No jobs were generated. Check database_ids and table_count.' if expected_job_count <= 0

      redis_store = MessageDeliverTest::RedisStatisticsStore.new(run_identifier)
      redis_store.initialize_run!(
        messages_per_user: messages_per_user,
        table_count: table_count,
        sender_user_id: sender_user_id,
        expected_job_count: expected_job_count,
        database_ids: selected_database_ids
      )

      selected_database_ids.each do |database_id|
        shard_configuration = configured_message_shards[database_id] || {}
        redis_store.store_database_metadata!(
          database_id: database_id,
          database_host: shard_configuration[:host],
          database_name: shard_configuration[:database]
        )
      end

      enqueue_worker_jobs(
        run_identifier: run_identifier,
        database_ids: selected_database_ids,
        table_count: table_count,
        messages_per_user: messages_per_user,
        sender_user_id: sender_user_id
      )
    end

    class << self
      def stringify_hash_keys(hash)
        normalized_hash = {}
        hash.each { |key, value| normalized_hash[key.to_s] = value }
        normalized_hash
      end

      def generated_run_identifier
        "mdt_#{Time.now.utc.strftime('%Y%m%d_%H%M%S')}_#{SecureRandom.hex(4)}"
      end
    end

    private

    def enqueue_worker_jobs(run_identifier:, database_ids:, table_count:, messages_per_user:, sender_user_id:)
      if Sidekiq::Client.respond_to?(:push_bulk)
        enqueue_worker_jobs_in_bulk(
          run_identifier: run_identifier,
          database_ids: database_ids,
          table_count: table_count,
          messages_per_user: messages_per_user,
          sender_user_id: sender_user_id
        )
      else
        enqueue_worker_jobs_individually(
          run_identifier: run_identifier,
          database_ids: database_ids,
          table_count: table_count,
          messages_per_user: messages_per_user,
          sender_user_id: sender_user_id
        )
      end
    end

    def enqueue_worker_jobs_in_bulk(run_identifier:, database_ids:, table_count:, messages_per_user:, sender_user_id:)
      pending_job_arguments = []

      database_ids.each do |database_id|
        table_count.times do |table_id|
          pending_job_arguments << [run_identifier, database_id, table_id, messages_per_user, sender_user_id]
          next unless pending_job_arguments.length >= SIDEKIQ_BULK_PUSH_BATCH_SIZE

          Sidekiq::Client.push_bulk(
            'class' => Jobs::SendMessageDeliverTestJob,
            'queue' => DEFAULT_SIDEKIQ_QUEUE,
            'args' => pending_job_arguments
          )
          pending_job_arguments = []
        end
      end

      return if pending_job_arguments.empty?

      Sidekiq::Client.push_bulk(
        'class' => Jobs::SendMessageDeliverTestJob,
        'queue' => DEFAULT_SIDEKIQ_QUEUE,
        'args' => pending_job_arguments
      )
    end

    def enqueue_worker_jobs_individually(run_identifier:, database_ids:, table_count:, messages_per_user:, sender_user_id:)
      database_ids.each do |database_id|
        table_count.times do |table_id|
          Jobs::SendMessageDeliverTestJob.perform_async(run_identifier, database_id, table_id, messages_per_user, sender_user_id)
        end
      end
    end

    def parse_database_ids(raw_database_ids)
      return [] if raw_database_ids.nil?
      return raw_database_ids.map(&:to_i).uniq if raw_database_ids.is_a?(Array)
      return raw_database_ids.split(',').map(&:to_i).uniq if raw_database_ids.is_a?(String)

      [raw_database_ids.to_i]
    end

    def default_database_ids
      database_ids = []
      configured_message_shards.each_with_index do |shard_settings, shard_index|
        next if EXCLUDED_SHARD_HOSTS.include?(shard_settings[:host])
        database_ids << shard_index
      end
      database_ids
    end

    def configured_message_shards
      CONFIG_MESSAGE_SHARDS[:message_shards] || []
    end

    def configured_table_count
      CONFIG_MESSAGE_SHARDS[:table_amount].to_i
    end

    def parse_positive_integer(raw_value, default_value)
      parsed_value = raw_value.to_i
      parsed_value.positive? ? parsed_value : default_value.to_i
    end
  end
end
