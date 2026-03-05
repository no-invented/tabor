# frozen_string_literal: true

require 'fileutils'
require 'time'

module Jobs
  class FinalizeMessageDeliverTestJob
    include Sidekiq::Worker
    include MessageDeliverTest::Constants

    REPORT_TABLE_COLUMN_WIDTHS = [6, 20, 12, 12, 12, 12, 14, 10].freeze
    REPORT_TABLE_HEADERS = ['DB ID', 'Host', 'Avg ms', 'Min ms', 'Max ms', 'Messages', 'Avg create s', 'Errors'].freeze

    sidekiq_options queue: DEFAULT_SIDEKIQ_QUEUE, retry: FINALIZE_JOB_SIDEKIQ_RETRY_COUNT

    def perform(run_identifier)
      redis_store = MessageDeliverTest::RedisStatisticsStore.new(run_identifier)
      run_metadata = redis_store.run_metadata
      database_report_rows = build_database_report_rows(redis_store)
      sorted_database_rows = database_report_rows.sort_by { |database_row| -database_row[:average_message_time_ms] }

      report_body = build_report_body(
        run_identifier: run_identifier,
        run_metadata: run_metadata,
        sorted_database_rows: sorted_database_rows,
        redis_store: redis_store
      )

      report_directory = File.join(Rails.root.to_s, REPORT_DIRECTORY_RELATIVE_PATH)
      FileUtils.mkdir_p(report_directory)

      safe_run_identifier = run_identifier.to_s.gsub(/[^a-zA-Z0-9\-_]/, '_')
      report_file_name = "#{REPORT_FILE_NAME_PREFIX}_#{safe_run_identifier}.txt"
      report_file_path = File.join(report_directory, report_file_name)

      File.open(report_file_path, 'wb') { |report_file| report_file.write(report_body) }
      redis_store.mark_run_as_finished!(report_file_path: report_file_path)
    end

    private

    def build_database_report_rows(redis_store)
      redis_store.database_ids.map do |database_id|
        raw_statistics = redis_store.database_statistics(database_id)
        raw_histogram = redis_store.database_histogram(database_id)

        total_messages_sent = raw_statistics[REDIS_FIELD_TOTAL_MESSAGES_SENT].to_i
        message_time_sum_ms = raw_statistics[REDIS_FIELD_MESSAGE_TIME_SUM_MS].to_f
        message_time_sum_of_squares_ms = raw_statistics[REDIS_FIELD_MESSAGE_TIME_SUM_OF_SQUARES_MS].to_f
        average_message_time_ms = total_messages_sent > 0 ? (message_time_sum_ms / total_messages_sent.to_f) : 0.0

        message_time_variance = if total_messages_sent > 0
                                  (message_time_sum_of_squares_ms / total_messages_sent.to_f) - (average_message_time_ms * average_message_time_ms)
                                else
                                  0.0
                                end
        message_time_variance = 0.0 if message_time_variance < 0.0

        users_total = raw_statistics[REDIS_FIELD_USERS_TOTAL].to_i
        users_with_sent_messages = raw_statistics[REDIS_FIELD_USERS_WITH_SENT_MESSAGES].to_i
        user_creation_time_sum_seconds = raw_statistics[REDIS_FIELD_USER_CREATION_TIME_SUM_SECONDS].to_f
        message_delivery_time_sum_seconds = raw_statistics[REDIS_FIELD_MESSAGE_DELIVERY_TIME_SUM_SECONDS].to_f

        {
          database_id: database_id,
          database_host: raw_statistics[REDIS_FIELD_DATABASE_HOST].to_s,
          database_name: raw_statistics[REDIS_FIELD_DATABASE_NAME].to_s,
          average_message_time_ms: average_message_time_ms,
          minimum_message_time_ms: raw_statistics[REDIS_FIELD_MESSAGE_TIME_MIN_MS].to_f,
          maximum_message_time_ms: raw_statistics[REDIS_FIELD_MESSAGE_TIME_MAX_MS].to_f,
          stddev_message_time_ms: Math.sqrt(message_time_variance),
          median_message_time_ms: approximate_percentile_ms_from_histogram(raw_histogram, total_messages_sent, PERCENTILE_MEDIAN),
          percentile_95_message_time_ms: approximate_percentile_ms_from_histogram(raw_histogram, total_messages_sent, PERCENTILE_95),
          total_messages_sent: total_messages_sent,
          total_errors: raw_statistics[REDIS_FIELD_TOTAL_MESSAGE_SEND_ERRORS].to_i,
          total_message_delivery_time_seconds: message_delivery_time_sum_seconds,
          messages_per_second: message_delivery_time_sum_seconds > 0.0 ? (total_messages_sent.to_f / message_delivery_time_sum_seconds) : 0.0,
          average_user_creation_time_seconds: users_total > 0 ? (user_creation_time_sum_seconds / users_total.to_f) : 0.0,
          users_total: users_total,
          users_with_sent_messages: users_with_sent_messages
        }
      end
    end

    def approximate_percentile_ms_from_histogram(raw_histogram, total_messages_count, percentile)
      return 0.0 if total_messages_count.to_i <= 0

      histogram = {}
      raw_histogram.each { |bucket_index, bucket_count| histogram[bucket_index.to_i] = bucket_count.to_i }

      target_rank = (total_messages_count.to_f * percentile / 100.0).ceil
      target_rank = 1 if target_rank < 1

      traversed_messages_count = 0
      (0..MESSAGE_TIME_HISTOGRAM_MAX_BUCKET_INDEX).each do |bucket_index|
        traversed_messages_count += histogram[bucket_index].to_i
        next if traversed_messages_count < target_rank

        bucket_start_ms = bucket_index * MESSAGE_TIME_HISTOGRAM_BUCKET_SIZE_MS
        return bucket_start_ms + (MESSAGE_TIME_HISTOGRAM_BUCKET_SIZE_MS / 2.0)
      end

      MESSAGE_TIME_HISTOGRAM_MAX_BUCKET_INDEX * MESSAGE_TIME_HISTOGRAM_BUCKET_SIZE_MS
    end

    def build_report_body(run_identifier:, run_metadata:, sorted_database_rows:, redis_store:)
      report_lines = []
      append_report_header(report_lines, run_identifier, run_metadata, redis_store)
      append_report_table(report_lines, sorted_database_rows)
      append_report_totals(report_lines, sorted_database_rows, run_metadata, redis_store)
      append_report_database_details(report_lines, sorted_database_rows)
      append_report_error_samples(report_lines, redis_store)
      report_lines.join("\n") + "\n"
    end

    def append_report_header(report_lines, run_identifier, run_metadata, redis_store)
      report_lines << ('=' * REPORT_HORIZONTAL_SEPARATOR_WIDTH)
      report_lines << 'MESSAGE DELIVERY LOAD TEST REPORT'
      report_lines << ('=' * REPORT_HORIZONTAL_SEPARATOR_WIDTH)
      report_lines << "Run identifier: #{run_identifier}"
      report_lines << "Started at: #{run_metadata[REDIS_FIELD_STARTED_AT]}"
      report_lines << "Finished at: #{Time.now.utc.iso8601}"
      report_lines << "Messages per user: #{run_metadata[REDIS_FIELD_MESSAGES_PER_USER]}"
      report_lines << "Expected jobs: #{run_metadata[REDIS_FIELD_EXPECTED_JOB_COUNT]}"
      report_lines << "Completed jobs: #{redis_store.completed_job_count}"
      report_lines << "Failed jobs: #{run_metadata[REDIS_FIELD_FAILED_JOB_COUNT].to_i}"
      report_lines << ('=' * REPORT_HORIZONTAL_SEPARATOR_WIDTH)
    end

    def append_report_table(report_lines, sorted_database_rows)
      report_lines << ''
      report_lines << 'DATABASE SUMMARY TABLE (slowest first)'
      report_lines << ('-' * REPORT_HORIZONTAL_SEPARATOR_WIDTH)
      report_lines << formatted_table_row(REPORT_TABLE_HEADERS, REPORT_TABLE_COLUMN_WIDTHS)
      report_lines << ('-' * REPORT_HORIZONTAL_SEPARATOR_WIDTH)

      sorted_database_rows.each do |database_row|
        report_lines << formatted_table_row(
          [
            database_row[:database_id],
            database_row[:database_host][0..19],
            format_number(database_row[:average_message_time_ms], 2),
            format_number(database_row[:minimum_message_time_ms], 2),
            format_number(database_row[:maximum_message_time_ms], 2),
            database_row[:total_messages_sent],
            format_number(database_row[:average_user_creation_time_seconds], 3),
            database_row[:total_errors]
          ],
          REPORT_TABLE_COLUMN_WIDTHS
        )
      end
      report_lines << ('-' * REPORT_HORIZONTAL_SEPARATOR_WIDTH)
    end

    def append_report_totals(report_lines, sorted_database_rows, run_metadata, redis_store)
      total_messages_sent = sorted_database_rows.map { |database_row| database_row[:total_messages_sent] }.inject(0, :+)
      total_errors = sorted_database_rows.map { |database_row| database_row[:total_errors] }.inject(0, :+)
      total_message_delivery_time_seconds = sorted_database_rows.map { |database_row| database_row[:total_message_delivery_time_seconds] }.inject(0.0, :+)

      report_lines << ''
      report_lines << ('=' * REPORT_HORIZONTAL_SEPARATOR_WIDTH)
      report_lines << 'OVERALL SUMMARY'
      report_lines << ('=' * REPORT_HORIZONTAL_SEPARATOR_WIDTH)
      report_lines << "Total messages sent: #{total_messages_sent}"
      report_lines << "Total errors: #{total_errors}"
      report_lines << "Total message delivery time (seconds): #{format_number(total_message_delivery_time_seconds, 2)}"
      report_lines << "Total messages per second: #{total_message_delivery_time_seconds > 0.0 ? format_number(total_messages_sent.to_f / total_message_delivery_time_seconds, 2) : '0.00'}"
      report_lines << "Expected jobs: #{run_metadata[REDIS_FIELD_EXPECTED_JOB_COUNT]}"
      report_lines << "Completed jobs: #{redis_store.completed_job_count}"
      report_lines << "Error events captured in Redis: #{redis_store.error_event_count}"
      report_lines << ('=' * REPORT_HORIZONTAL_SEPARATOR_WIDTH)
    end

    def append_report_database_details(report_lines, sorted_database_rows)
      report_lines << ''
      report_lines << 'DATABASE DETAILS (first 10 by slowness)'
      report_lines << ('=' * REPORT_HORIZONTAL_SEPARATOR_WIDTH)

      sorted_database_rows.first(REPORT_DETAIL_SECTION_DATABASE_LIMIT).each_with_index do |database_row, index|
        report_lines << "[#{index + 1}] DB##{database_row[:database_id]} #{database_row[:database_host]}/#{database_row[:database_name]}"
        report_lines << "  Average message time (ms): #{format_number(database_row[:average_message_time_ms], 2)}"
        report_lines << "  Median message time (ms): #{format_number(database_row[:median_message_time_ms], 2)}"
        report_lines << "  95th percentile time (ms): #{format_number(database_row[:percentile_95_message_time_ms], 2)}"
        report_lines << "  Min/Max message time (ms): #{format_number(database_row[:minimum_message_time_ms], 2)} / #{format_number(database_row[:maximum_message_time_ms], 2)}"
        report_lines << "  Standard deviation (ms): #{format_number(database_row[:stddev_message_time_ms], 2)}"
        report_lines << "  Messages sent: #{database_row[:total_messages_sent]}"
        report_lines << "  Users with sent messages: #{database_row[:users_with_sent_messages]} of #{database_row[:users_total]}"
        report_lines << "  Errors: #{database_row[:total_errors]}"
        report_lines << "  Message throughput (msg/s): #{format_number(database_row[:messages_per_second], 2)}"
        report_lines << "  Average user creation time (s): #{format_number(database_row[:average_user_creation_time_seconds], 3)}"
        report_lines << ''
      end
    end

    def append_report_error_samples(report_lines, redis_store)
      report_lines << 'ERROR SAMPLES'
      report_lines << ('=' * REPORT_HORIZONTAL_SEPARATOR_WIDTH)
      redis_store.error_events(limit: REPORT_ERROR_EVENTS_SAMPLE_LIMIT).each_with_index do |error_event, index|
        report_lines << "#{index + 1}. [db=#{error_event['database_id']} table=#{error_event['table_id']} stage=#{error_event['failed_stage']}] #{error_event['error_class']}: #{error_event['error_message']}"
      end
      report_lines << ('=' * REPORT_HORIZONTAL_SEPARATOR_WIDTH)
    end

    def formatted_table_row(columns, widths)
      row = '|'
      columns.each_with_index do |column, column_index|
        row << " #{column.to_s.ljust(widths[column_index])} |"
      end
      row
    end

    def format_number(number, precision)
      format("%.#{precision}f", number.to_f)
    end
  end
end
