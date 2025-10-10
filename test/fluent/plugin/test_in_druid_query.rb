# frozen_string_literal: true

require 'helper'

require 'fluent/plugin/in_druid_query'

class DruidQueryInputTest < Test::Unit::TestCase
  BASE_CONF = [].freeze

  SQL_QUERY = 'select * from test'

  QUERY_CONF = [
    '<query>',
    "sql #{SQL_QUERY}",
    '</query>'
  ].freeze

  TEST_TIME = '2025-01-01T00:00:00.000Z'
  TEST_FLUENT_TIME = Fluent::EventTime.parse(TEST_TIME)

  setup do
    Fluent::Test.setup
  end

  sub_test_case 'configuration' do
    test 'default configuration' do
      driver = create_driver
      input = driver.instance

      assert_equal Fluent::Plugin::DruidQueryInput::DEFAULT_TAG, input.tag
      assert_equal Fluent::Plugin::DruidQueryInput::DEFAULT_TAG, input.tag_info

      assert_equal Fluent::Plugin::DruidQueryInput::DEFAULT_INTERVAL, input.interval

      assert_equal Fluent::Plugin::DruidQueryInput::DEFAULT_DRUID_URL, input.druid_url
      assert_equal Fluent::Plugin::DruidQueryInput::DEFAULT_DRUID_USER, input.druid_user
      assert_equal Fluent::Plugin::DruidQueryInput::DEFAULT_DRUID_PASSWORD, input.druid_password
      assert_equal Fluent::Plugin::DruidQueryInput::DEFAULT_DRUID_VERIFY_SSL, input.druid_verify_ssl
      assert_equal Fluent::Plugin::DruidQueryInput::DEFAULT_DRUID_USER_AGENT, input.druid_user_agent
      assert_equal Fluent::Plugin::DruidQueryInput::DEFAULT_DRUID_TIMEOUT, input.druid_timeout

      assert_equal Fluent::Plugin::DruidQueryInput::DEFAULT_CA_CERT, input.ca_cert
    end

    test 'tag should not be empty' do
      test_conf = generate_conf(base_conf: ['tag  '])
      assert_raise(Fluent::ConfigError) do
        create_driver(test_conf)
      end
    end

    test 'tag_info should not be empty' do
      test_conf = generate_conf(base_conf: ['tag_info  '])
      assert_raise(Fluent::ConfigError) do
        create_driver(test_conf)
      end
    end

    sub_test_case 'sql queries' do
      test 'sql queries should not be empty' do
        test_conf = generate_conf(query_conf: [])
        assert_raise(Fluent::ConfigError) do
          create_driver(test_conf)
        end
      end
    end
  end

  sub_test_case 'run_queries' do
    test 'it call druid_client with expected query' do
      driver = create_driver
      input = driver.instance

      input.druid_client.sql.expects(:query).with(
        query: SQL_QUERY,
        header: false,
        context: {
          useCache: true,
          populateCache: true
        }
      ).returns([])
      input.run_queries
    end

    test 'it emits returned records' do
      driver = create_driver
      input = driver.instance

      records = [{ test1: 'test1' }, { test2: 'test2' }]
      response = DruidClient::Api::Response.new(
        status_code: 200,
        body: records,
        duration: -1
      )
      input.druid_client.sql.expects(:query).with(
        query: SQL_QUERY,
        header: false,
        context: {
          useCache: true,
          populateCache: true
        }
      ).returns(response)

      input.run_queries
      emitted_events = driver.events
      assert_equal 2, emitted_events.size
      emitted_events.each { |emitted_event| assert_equal input.tag, emitted_event[0] }
      emitted_records = emitted_events.map { |emitted_event| emitted_event[2] }
      assert_equal records, emitted_records
    end

    test 'it emits returned records on specified tag' do
      test_query_conf = [
        '<query>',
        "sql #{SQL_QUERY}",
        'subtag test',
        '</query>'
      ]

      test_conf = generate_conf(query_conf: test_query_conf)
      driver = create_driver(test_conf)
      input = driver.instance

      records = [{ test3: 'test3' }]
      response = DruidClient::Api::Response.new(
        status_code: 200,
        body: records,
        duration: -1
      )
      input.druid_client.sql.expects(:query).with(
        query: SQL_QUERY,
        header: false,
        context: {
          useCache: true,
          populateCache: true
        }
      ).returns(response)

      input.run_queries
      emitted_events = driver.events

      assert_equal 1, emitted_events.size
      assert_equal [input.tag, 'test'].join('.'), emitted_events.first.first

      emitted_records = emitted_events.map { |emitted_event| emitted_event[2] }
      assert_equal records, emitted_records
    end

    test 'it emits returned records with metadata' do
      test_query_conf = [
        '<query>',
        "sql #{SQL_QUERY}",
        'metadata query_id:1',
        '</query>'
      ]

      test_conf = generate_conf(query_conf: test_query_conf)
      driver = create_driver(test_conf)
      input = driver.instance

      records = [{ test3: 'test3' }]
      response = DruidClient::Api::Response.new(
        status_code: 200,
        body: records,
        duration: -1
      )
      input.druid_client.sql.expects(:query).with(
        query: SQL_QUERY,
        header: false,
        context: {
          useCache: true,
          populateCache: true
        }
      ).returns(response)

      input.run_queries
      emitted_events = driver.events

      assert_equal 1, emitted_events.size
      emitted_events.each_with_index do |emitted_event, i|
        assert_equal emitted_event[0], input.tag
        assert_equal emitted_event[2], records[i].merge({ 'query_id' => '1' })
      end
    end

    test 'it emits returned response information' do
      test_query_conf = [
        '<query>',
        "sql #{SQL_QUERY}",
        'generate_record false',
        'generate_info true',
        '</query>'
      ]
      test_conf = generate_conf(query_conf: test_query_conf)
      driver = create_driver(test_conf)
      input = driver.instance

      records = [{ test3: 'test3' }]
      response = DruidClient::Api::Response.new(
        status_code: 200,
        body: records,
        duration: -1
      )
      input.druid_client.sql.expects(:query).with(
        query: SQL_QUERY,
        header: false,
        context: {
          useCache: true,
          populateCache: true
        }
      ).returns(response)

      Fluent::EventTime.stubs(:now).returns(TEST_FLUENT_TIME)
      input.run_queries
      emitted_events = driver.events

      assert_equal 1, emitted_events.size
      assert_equal emitted_events[0][0], input.tag
      assert_equal emitted_events[0][2], {
        'query_duration' => -1,
        'status' => 'success',
        'status_code' => 200,
        'response_rows_count' => 1,
        'timestamp' => TEST_FLUENT_TIME.to_time.utc.iso8601(3)
      }
    end
  end

  private

  def generate_conf(base_conf: BASE_CONF, extra_conf: [], query_conf: QUERY_CONF)
    (base_conf + extra_conf + query_conf).join("\n")
  end

  def create_driver(conf = generate_conf)
    Fluent::Test::Driver::Input.new(Fluent::Plugin::DruidQueryInput).configure(conf)
  end
end
