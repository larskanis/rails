module ActiveRecord
  module ConnectionAdapters
    module PostgreSQL
      module OID # :nodoc:
        class Array < Type::Value # :nodoc:
          class ElementEncoder < PG::SimpleEncoder
            def initialize(type)
              @type = type
            end

            def encode(value)
              @type.type_cast_for_database(value).to_s
            end
          end

          class ElementDecoder < PG::SimpleDecoder
            def initialize(type)
              @type = type
            end

            def decode(string, tuple=nil, field=nil)
              @type.type_cast_from_database(string)
            end
          end

          include Type::Mutable

          attr_reader :subtype, :delimiter
          delegate :type, to: :subtype

          def initialize(subtype, delimiter = ',')
            @subtype = subtype
            @delimiter = delimiter

            pg_elem_encoder = @subtype.respond_to?(:pg_encoder) ? @subtype.pg_encoder : ElementEncoder.new(@subtype)
            pg_elem_decoder = @subtype.respond_to?(:pg_decoder) ? @subtype.pg_decoder : ElementDecoder.new(@subtype)

            @pg_encoder = PG::TextEncoder::Array.new name: "#{type}[]", elements_type: pg_elem_encoder, delimiter: delimiter
            @pg_decoder = PG::TextDecoder::Array.new name: "#{type}[]", elements_type: pg_elem_decoder, delimiter: delimiter
          end

          def type_cast_from_database(value)
            if value.is_a?(::String)
              @pg_decoder.decode(value)
            else
              super
            end
          end

          def type_cast_from_user(value)
            type_cast_array(value, :type_cast_from_user)
          end

          def type_cast_for_database(value)
            if value.is_a?(::Array)
              @pg_encoder.encode(value)
            else
              super
            end
          end

          private

          def type_cast_array(value, method)
            if value.is_a?(::Array)
              value.map { |item| type_cast_array(item, method) }
            else
              @subtype.public_send(method, value)
            end
          end
        end
      end
    end
  end
end
