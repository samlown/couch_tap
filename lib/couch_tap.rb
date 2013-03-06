
# Low level requirements
require 'sequel'
require 'couchrest'

# Our stuff
require 'couch_tap/changes'
require 'couch_tap/document'
require 'couch_tap/document/table'


module CouchTap
  extend self

  def changes(database, &block)
    Changes.new(database, &block)
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

