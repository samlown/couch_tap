
require 'test_helper'

class Destroyers::TableTest < Test::Unit::TestCase

  def setup
    @database = create_database
  end


  protected

  def create_database
    database = Sequel.sqlite
    database.create_table :items do
      String :id
      String :name
      Time :created_at
      index :id, :unique => true
    end
    database
  end


end
