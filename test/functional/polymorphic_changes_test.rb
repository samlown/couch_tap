require 'test_helper'

class PolymorphicChangesTest < Test::Unit::TestCase

  def setup
    stub_request(:get, TEST_DB_ROOT).
      with(headers: { 'Accept'=>'application/json', 'Content-Type'=>'application/json' }).
      to_return(status: 200, body: { db_name: TEST_DB_NAME }.to_json , headers: {})
  end

  def test_inserting_rows
    docs = [
      {'seq' => 1, 'id' => '1234', 'doc' => {
        '_id' => '1234', 'type' => 'Parent1', 'name' => 'Parent 1 name', 'child' => {'name' => 'Parent 1 child'}
      }},
      {'seq' => 2, 'id' => '1234', 'doc' => {
        '_id' => '1234', 'type' => 'Parent2', 'name' => 'Parent 2 name', 'child' => {'name' => 'Parent 2 child'}
      }}
    ]

    changes = config_changes batch_size: 2

    docs.each { |doc| changes.send(:process_row, doc) }

    changes.stop_consumer

    assert_equal 1, @database[:parent1].count
    assert_equal 1, @database[:parent2].count
    children = @database[:child].to_a
    assert_equal 2, children.count
    assert_includes children, parent1_id: '1234', parent2_id: nil, name: 'Parent 1 child'
    assert_includes children, parent1_id: nil, parent2_id: '1234', name: 'Parent 2 child'
  end

  def test_delete
    doc = { "id" => "1234", "seq" => 3, "deleted" => true }

    changes = config_changes batch_size: 4

    @database[:parent1].insert(parent1_id: '1234', name: 'Parent 1')
    @database[:parent2].insert(parent2_id: '1234', name: 'Parent 2')
    @database[:child].insert(parent1_id: '1234', name: 'Parent 1 child')
    @database[:child].insert(parent2_id: '1234', name: 'Parent 2 child')

    changes.send(:process_row, doc)
    changes.stop_consumer

    assert_equal 0, @database[:parent1].count
    assert_equal 0, @database[:parent2].count
    assert_equal 0, @database[:child].count
  end

  protected

  def config_changes(opts)
    changes = CouchTap::Changes.new(couch_db: TEST_DB_ROOT, timeout: 60) do
      database db: 'sqlite:/', batch_size: opts.fetch(:batch_size)

      document type: 'Parent1' do
        table :parent1 do
          collection :child do
            table :child do
            end
          end
        end
      end
      document type: 'Parent2' do
        table :parent2 do
          collection :child do
            table :child do
            end
          end
        end
      end
    end

    @database = changes.query_executor.database
    migrate_sample_database @database

    changes.send(:start_consumer)
    return changes
  end

  def migrate_sample_database(connection)
    connection.create_table :parent1 do
      String :parent1_id
      String :name
    end

    connection.create_table :parent2 do
      String :parent2_id
      String :name
    end

    connection.create_table :child do
      String :parent1_id
      String :parent2_id
      String :name
    end
  end
end
