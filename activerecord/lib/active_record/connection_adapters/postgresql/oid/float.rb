module ActiveRecord
  module ConnectionAdapters
    module PostgreSQL
      module OID # :nodoc:
        class Float < Type::Float # :nodoc:
          include Infinity

          attr_reader :pg_encoder
          attr_reader :pg_decoder

          def initialize(options = {})
            super
            @pg_encoder = PG::TextEncoder::Float.new name: type
            @pg_decoder = PG::TextDecoder::Float.new name: type
          end
        end
      end
    end
  end
end
