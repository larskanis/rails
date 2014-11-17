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

          def type_cast_from_database(value)
            return if value.nil?
            value = super
            # Decoded bytea values from the database are always binary, but if
            # the value is passed from user space, like in
            # PostgresqlByteaTest#test_type_cast_binary_converts_the_encoding,
            # we need to make it binary explicitly.
            value.to_s.force_encoding(Encoding::BINARY) unless value.encoding == Encoding::BINARY
            value
          end
        end
      end
    end
  end
end
