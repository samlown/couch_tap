
# Low level requirements
require 'sequel'
require 'couchrest'
require 'em-http'
require 'yajl'
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


module CouchTap
  extend self

  def changes(database, &block)
    (@changes ||= []) << Changes.new(database, &block)
  end

  def start
    EventMachine.run do
      @changes.each do |changes|
        changes.start
      end
    end
  end

  # Provide some way to handle messages
  def logger
    @logger ||= prepare_logger
  end

  def prepare_logger
    log = Logger.new(STDOUT)
    log.level = Logger::INFO
    log
  end

end

