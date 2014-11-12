module ActiveRecord
  module ConnectionAdapters
    module PostgreSQL
      module OID # :nodoc:
        class Bytea < Type::Binary # :nodoc:
          attr_reader :pg_encoder
          attr_reader :pg_decoder

          def initialize(options = {})
            super
            @pg_encoder = PG::BinaryEncoder::Bytea.new name: type, format: 1
            @pg_decoder = PG::TextDecoder::Bytea.new name: type
          end
        end
      end
    end
  end
end
