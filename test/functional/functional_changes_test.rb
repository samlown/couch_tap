require 'test_helper'

class FunctionalChangesTest < Test::Unit::TestCase

  def test_insert_sales_and_nested_entries
    changes = CouchTap::Changes.new('couch_tap') do 
      database db: 'sqlite:/', batch_size: 1

      document :type => 'Sale' do
        table :sales do
          column :audited_at, Time.now
          collection :entries do
            table :sale_entries, primary_key: false do
              column :audited_at, Time.now
            end
          end
        end
      end
    end

    migrate_sample_database changes.database

    changes.send(:process_row, { "id" => 1, "seq" => 111, "doc" => { "_id" => "50", "type" => "Sale", "code" => "Code 1", "amount" => 600, "entries" => [{ "price" => 500 }, { "price" => 100 }] }})

    assert_equal 1, changes.database[:sales].count
    assert_equal 2, changes.database[:sale_entries].count
  end

  def test_insert_and_update_sales_and_nested_entries_in_same_batch
    changes = CouchTap::Changes.new('couch_tap') do
      database db: 'sqlite:/', batch_size: 10

      document :type => 'Sale' do
        table :sales do
          column :audited_at, Time.now
          collection :entries do
            table :sale_entries, primary_key: false do
              column :audited_at, Time.now
            end
          end
        end
      end
    end

    migrate_sample_database changes.database

    changes.send(:process_row, { "id" => 1, "seq" => 111, "doc" => { "_id" => "50", "type" => "Sale", "code" => "Code 1", "amount" => 600, "entries" => [{ "price" => 500 }, { "price" => 100 }] }})
    changes.send(:process_row, { "id" => 2, "seq" => 112, "doc" => { "_id" => "50", "type" => "Sale", "code" => "Code 2", "amount" => 800, "entries" => [{ "price" => 50 }, { "price" => 750 }] }})

    assert_equal 1, changes.database[:sales].count
    assert_equal 2, changes.database[:sale_entries].count
  end


  def test_delete_children
    changes = CouchTap::Changes.new('couch_tap') do
      database db: 'sqlite:/', batch_size: 1

      document :type => 'Sale' do
        table :sales do
          column :audited_at, Time.now
          collection :entries do
            table :sale_entries, primary_key: false do
              column :audited_at, Time.now
            end
          end
        end
      end
    end

    migrate_sample_database changes.database

    changes.send(:process_row, { "id" => 1, "seq" => 111, "doc" => { "_id" => "50", "type" => "Sale", "code" => "Code 1", "amount" => 600, "entries" => [{ "price" => 500 }, { "price" => 100 }] }})
    changes.send(:process_row, { "id" => "50", "seq" => 112, "deleted" => true } ) 

    assert_equal 0, changes.database[:sales].count
    assert_equal 0, changes.database[:sale_entries].count
  end

  protected

  def migrate_sample_database(connection)
    connection.create_table :sales do
      String :sale_id
      String :code
      Float :amount
    end

    connection.create_table :sale_entries do
      String :sale_id
      String :price
    end
  end
end
