# frozen_string_literal: true

require 'helper'
require 'fluent/plugin/in_druid_query'

class DruidQueryInputTest < Test::Unit::TestCase
  setup do
    Fluent::Test.setup
  end

  test 'OK' do
    puts :OK
  end

  private

  def create_driver(conf)
    Fluent::Test::Driver::Input.new(Fluent::Plugin::DruidQueryInput).configure(conf)
  end
end
