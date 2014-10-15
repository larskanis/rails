module ActiveRecord
  module ConnectionAdapters
    module PostgreSQL
      module OID # :nodoc:
        class Integer < Type::Integer # :nodoc:
          include Infinity
          include PgEncoder
          include PgDecoder

          def initialize(options = {})
            super
            @pg_encoder = PG::TextEncoder::Integer.new name: type
            @pg_decoder = PG::TextDecoder::Integer.new name: type
          end
        end
      end
    end
  end
end
