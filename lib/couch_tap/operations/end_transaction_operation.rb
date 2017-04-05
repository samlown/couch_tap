
module CouchTap
  module Operations
    class EndTransactionOperation
      attr_reader :sequence

      def initialize(sequence)
        @sequence = sequence
      end

      def ==(other)
        other.is_a?(EndTransactionOperation) &&
          sequence == other.sequence
      end
    end
  end
end
