
module CouchTap
  module Operations
    class BeginTransactionOperation
      def ==(other)
        other.is_a? BeginTransactionOperation
      end
    end
  end
end

