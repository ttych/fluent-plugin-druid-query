# frozen_string_literal: true

#
# Copyright 2025- Thomas Tych
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'fluent/plugin/input'

require 'druid_client'

module Fluent
  module Plugin
    class DruidQueryInput < Fluent::Plugin::Input
      NAME = 'druid_query'
      Fluent::Plugin.register_input(NAME, self)

      helpers :event_emitter, :timer

      DEFAULT_TAG = NAME
      DEFAULT_INTERVAL = 300

      DEFAULT_DRUID_URL = 'http://localhost:8888'
      DEFAULT_DRUID_USER = nil
      DEFAULT_DRUID_PASSWORD = nil
      DEFAULT_DRUID_VERIFY_SSL = true
      DEFAULT_DRUID_USER_AGENT = NAME
      DEFAULT_DRUID_TIMEOUT = 30

      DEFAULT_CA_CERT = nil

      DEFAULT_QUERY_CACHE = true
      DEFAULT_QUERY_SUBTAG = nil
      DEFAULT_QUERY_GENERATE_RECORD = true
      DEFAULT_QUERY_GENERATE_INFO = false

      desc 'tag to emit events on'
      config_param :tag, :string, default: DEFAULT_TAG
      desc 'info tag to emit info events on'
      config_param :tag_info, :string, default: DEFAULT_TAG
      desc 'interval for probe execution'
      config_param :interval, :time, default: DEFAULT_INTERVAL

      desc 'druid url'
      config_param :druid_url, :string, default: DEFAULT_DRUID_URL
      desc 'druid user'
      config_param :druid_user, :string, default: DEFAULT_DRUID_USER
      desc 'druid password'
      config_param :druid_password, :string, default: DEFAULT_DRUID_PASSWORD
      desc 'druid verify ssl'
      config_param :druid_verify_ssl, :bool, default: DEFAULT_DRUID_VERIFY_SSL
      desc 'druid user agent'
      config_param :druid_user_agent, :string, default: DEFAULT_DRUID_USER_AGENT
      desc 'druid connection timeout'
      config_param :druid_timeout, :integer, default: DEFAULT_DRUID_TIMEOUT

      desc 'ca_cert'
      config_param :ca_cert, :string, default: DEFAULT_CA_CERT

      config_section :query, param_name: :queries, multi: true do
        config_param :sql, :string
        config_param :cache, :bool, default: DEFAULT_QUERY_CACHE
        config_param :subtag, :string, default: DEFAULT_QUERY_SUBTAG
        config_param :metadata, :hash, value_type: :string, default: {}
        config_param :generate_record, :bool, default: DEFAULT_QUERY_GENERATE_RECORD
        config_param :generate_info, :bool, default: DEFAULT_QUERY_GENERATE_INFO
      end

      def configure(conf)
        super

        raise Fluent::ConfigError, 'tag should not be empty' if tag.empty?
        raise Fluent::ConfigError, 'tag_info should not be empty' if tag_info.empty?

        check_druid_information
        check_druid_queries
      end

      def check_druid_information
        raise Fluent::ConfigError, 'druid_url should not be empty' if druid_url.empty?
      end

      def check_druid_queries
        raise Fluent::ConfigError, 'queries should not be empty' if queries.empty?
      end

      def start
        super

        timer_execute(:run_queries_first, 1, repeat: false, &method(:run_queries)) if interval > 60
        timer_execute(:run_queries, interval, repeat: true, &method(:run_queries))
      end

      def run_queries
        queries.each do |query|
          run_query(query)
        rescue StandardError => e
          log.error "while runnig query: #{query.sql}: #{e}"
        end
      end

      def run_query(query)
        query_time = Fluent::Engine.now
        response = druid_client.sql.query(
          query: query.sql,
          header: false,
          context: query_cache_context(use_cache: query.cache)
        )
        emit_query_records(query_time: query_time, query: query, response: response)
        emit_query_info(query_time: query_time, query: query, response: response)
      end

      def query_cache_context(use_cache: true)
        {
          useCache: use_cache,
          populateCache: use_cache
        }
      end

      def druid_client
        @druid_client ||= DruidClient::Api.new(
          url: druid_url,
          username: druid_user,
          password: druid_password,
          user_agent: druid_user_agent,
          verify_ssl: druid_verify_ssl,
          timeout: druid_timeout,
          ca_file: ca_cert,
          log: log
        )
      end

      def emit_query_records(query:, response:, query_time: Fluent::Engine.now)
        return unless query.generate_record
        return unless response.success?

        current_tag = [tag, query.subtag].compact.join('.')
        query_events = MultiEventStream.new
        response.body.each do |response_entry|
          query_events.add(query_time, response_entry.merge(query.metadata))
        end
        router.emit_stream(current_tag, query_events)
      end

      def emit_query_info(query:, response:, query_time: Fluent::Engine.now)
        return unless query.generate_info

        current_tag = [tag_info, query.subtag].compact.join('.')
        info_record = {
          'timestamp' => query_time.to_time.utc.iso8601(3),
          'status' => response.success? ? 'success' : 'failure',
          'status_code' => response.status_code,
          'query_duration' => response.duration,
          'response_rows_count' => response.body.size
        }.merge(query.metadata)
        router.emit(current_tag, query_time, info_record)
      end
    end
  end
end
