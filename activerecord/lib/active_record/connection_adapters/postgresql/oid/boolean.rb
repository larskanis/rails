module ActiveRecord
  module ConnectionAdapters
    module PostgreSQL
      module OID # :nodoc:
        class Boolean < Type::Boolean # :nodoc:
          include PgEncoder
          include PgDecoder

          def initialize(options = {})
            super
            @pg_encoder = PG::TextEncoder::Boolean.new name: type
            @pg_decoder = PG::TextDecoder::Boolean.new name: type
          end
        end
      end
    end
  end
end
