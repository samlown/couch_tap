
$LOAD_PATH.unshift(File.dirname(__FILE__))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), "..", "lib"))

require 'test/unit'
require 'mocha/setup'
require 'couch_tap'
require 'byebug'
require 'timecop'
require 'webmock/test_unit'

TEST_DB_HOST = 'http://127.0.0.1:5984/'
TEST_DB_NAME = 'couch_tap'
TEST_DB_ROOT = File.join(TEST_DB_HOST, TEST_DB_NAME)
TEST_DB = CouchRest.database(TEST_DB_ROOT)

def reset_test_db!
  WebMock.allow_net_connect!
  TEST_DB.recreate!
  WebMock.disable_net_connect!
end

ENV['LOG_LEVEL'] ||= 'debug'
