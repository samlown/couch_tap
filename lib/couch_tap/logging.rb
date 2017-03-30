
module Logging

  # Provide some way to handle messages
  def logger
    @logger ||= prepare_logger
  end

  def prepare_logger
    log = Logger.new(STDOUT)
    log.level = level
    log
  end

  private

  def level
    lvl = ENV.fetch('LOG_LEVEL', 'info')

    case lvl
    when 'unknown'
      Logger::UNKNOWN
    when 'fatal'
      Logger::FATAL
    when 'error'
      Logger::ERROR
    when 'warn'
      Logger::WARN
    when 'info'
      Logger::INFO
    when 'debug'
      Logger::DEBUG
    else
      raise "Unknown log level: #{lvl}"
    end
  end
end
