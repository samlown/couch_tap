
# Low level requirements
require 'sequel'
require 'couchrest'
require 'yajl'
require 'httpclient'
require 'logger'
require 'active_support/inflector'
require 'active_support/core_ext/object/blank'

# Our stuff
require 'couch_tap/changes'
require 'couch_tap/schema'
require 'couch_tap/document_handler'
require 'couch_tap/builders/collection'
require 'couch_tap/builders/table'
require 'couch_tap/destroyers/collection'
require 'couch_tap/destroyers/table'
require 'couch_tap/query_executor'
require 'couch_tap/logging'
require 'couch_tap/timer'
require 'couch_tap/operations/insert_operation'
require 'couch_tap/operations/delete_operation'
require 'couch_tap/operations/begin_transaction_operation'
require 'couch_tap/operations/end_transaction_operation'
require 'couch_tap/operations/close_queue_operation'
require 'couch_tap/operations/timer_fired_signal'
require 'couch_tap/operations_queue'

module CouchTap
  extend self
  extend Logging

  def changes(database, &block)
    (@changes ||= []) << Changes.new(database, &block)
  end

  def start
    threads = []
    @changes.each do |changes|
      t = Thread.new(changes) do |c|
        c.start
      end
      t.abort_on_exception = true
      threads << t
    end
    threads.each {|thr| thr.join}
  end

  def stop
    @changes.each { |c| c.stop }
  end
end

