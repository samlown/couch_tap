
#require 'logging'
require 'logging/logstash'

module CouchTap
  module LogFactory
    LOG_LAYOUT = Logging.layouts.pattern(:pattern => "%.1l [%d] %5l - %c: %m\n")

    def self.config_loggers()
      level = parse_level(ENV.fetch('LOG_LEVEL', 'info'))
      Logging.appenders.stdout('stdout', 
                               :level => level, 
                               :layout => LOG_LAYOUT)
      Logging.logger.root.add_appenders('stdout')
      if ENV.fetch('USE_LOGSTASH', 'false') == "true"
        configure_logstash()
      end
    end

    def self.configure_logstash()
      logstash_host = ENV.fetch('LOGSTASH_HOST', 'localhost')
      logstash_port = ENV.fetch('LOGSTASH_PORT', '5016')
      level = parse_level(ENV.fetch('LOGSTASH_LOG_LEVEL', 'debug'))
      ssl_enable = parse_level(ENV.fetch('LOGSTASH_SSL_ENABLE', 'debug'))
      Logging.appenders.logstash('logstash', 
                                 :level => level,
                                 :layout => LOG_LAYOUT,
                                 :ssl_enable => ssl_enable,
                                 :uri => "tcp://#{logstash_host}:#{logstash_port}")
      Logging.logger.root.add_appenders('logstash')
    end

    def self.parse_level(level)
      case level
      when 'unknown' then :unknown
      when 'fatal' then :fatal
      when 'error' then :error
      when 'warn' then :warn
      when 'info' then :info
      when 'debug' then :debug
      else
        raise "Unknown log level: #{level}"
      end
    end
  end
end
