require 'cases/helper'
require 'minitest/mock'

module ActiveRecord
  module ConnectionAdapters
    class PostgreSQLAdapter < AbstractAdapter
      class PreparedStatementPoolTest < ActiveRecord::TestCase
        if Process.respond_to?(:fork)
          def test_cache_is_per_pid
            cache = PreparedStatementPool.new nil, 10
            cache.add('foo', 'bar')
            assert_equal PreparedStatementPool::PoolEntry.new('a1'), cache['foo']

            pid = fork {
              lookup = cache['foo'];
              exit!(!lookup)
            }

            Process.waitpid pid
            assert $?.success?, 'process should exit successfully'
          end
        end

        def test_send_prepare_statements
          conn = Minitest::Mock.new
          cache = PreparedStatementPool.new conn, 10
          cache.add("stmt", "SQL")

          conn.expect :send_prepare, nil, ['a1', 'SQL']
          cache.send_pending_query

          conn.expect :get_last_result, nil
          cache.finish_pending_query
          conn.verify
        end

        def with_filled_cache(conn, count)
          cache = PreparedStatementPool.new conn, 10

          count.times do |idx|
            cache.add("stmt #{idx}", "SQL #{idx}")
            cache.discard_pending_query
          end

          yield cache
        end

        def test_deletes_least_recently_used
          conn = Minitest::Mock.new
          with_filled_cache(conn, 10) do |cache|
            cache['stmt 1']

            cache.delete_oversized
            conn.expect :send_query, nil, ['DEALLOCATE a1;DEALLOCATE a3;']
            cache.send_pending_query
          end
          conn.verify
        end

        def test_bundles_dealloc_statements_when_the_limit_is_reached
          conn = Minitest::Mock.new

          with_filled_cache(conn, 9) do |cache|
            cache.delete_oversized
            assert_equal 9, cache.length, "pool limit should not be reached"

            cache.add("stmt 10", "SQL 10")
            cache.discard_pending_query
            assert_equal 10, cache.length

            cache.delete_oversized
            assert_operator 8, :<=, cache.length

            conn.expect :send_query, nil, ['DEALLOCATE a1;DEALLOCATE a2;']
            conn.expect :get_last_result, nil
            cache.execute_pending_query
          end

          conn.verify
        end

        def test_error_on_two_statements
          cache = PreparedStatementPool.new nil, 10
          cache.add("stmt 1", "SQL 1")
          assert_raises(ArgumentError) do
            cache.add("stmt 2", "SQL 2")
          end
        end
      end
    end
  end
end
