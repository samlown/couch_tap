
require 'datadog/statsd'

module CouchTap
  class Metrics

    def initialize(tags = {})
      @statsd = Datadog::Statsd.new('localhost', 8125)
      @tags = tags
    end

    def increment(key, tags = {})
      @statsd.increment(key, tags: tags.merge(@tags))
    end

    def histogram(key, value, tags = {})
      @statsd.histogram(key, value, tags: tags.merge(@tags))
    end
  end
end

