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

          def type_cast_from_user(value)
            case value
            when ::Float then     value
            when 'Infinity' then  ::Float::INFINITY
            when '-Infinity' then -::Float::INFINITY
            when 'NaN' then       ::Float::NAN
            else                  value.to_f
            end
          end
        end
      end
    end
  end
end
