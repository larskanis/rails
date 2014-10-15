module ActiveRecord
  module ConnectionAdapters
    module PostgreSQL
      module OID # :nodoc:
        class Bytea < Type::Binary # :nodoc:
          include PgEncoder
          include PgDecoder

          def initialize(options = {})
            super
            @pg_encoder = PG::BinaryEncoder::Bytea.new name: type, format: 1
            @pg_decoder = PG::TextDecoder::Bytea.new name: type
          end

          def type_cast_from_database(value)
            return if value.nil?
            value = super
            value.to_s.force_encoding(Encoding::BINARY) unless value.encoding == Encoding::BINARY
            value
          end
        end
      end
    end
  end
end
