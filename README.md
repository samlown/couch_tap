
# Couch Tap

Utility to listen to a CouchDB changes feed and automatically insert, update,
or delete rows matching key-value conditions of incomning documents into a 
relational database.

While CouchDB is awesome, business people probably won't be
quite as impressed when they want to play around with the data. Regular SQL
is generally accepted as being easy to use and much more widely supported by a larger
range of comercial tools, such as chart.io.

Couch Tap will listen to incoming documents on a CouchDB's changes
stream and automatically update rows of RDBMS tables defined in the
conversion schema. The changes stream uses a sequence number allowing
synchronisation to be started and stopped at will.

Ruby's fast and simple (sequel)[http://sequel.jeremyevans.net/] library is used to provide the connection to the
database. This library can also be used for migrations, important for frequently changing schemas.

Couch tap takes a simple two-step approach converting documents to rows. When a change event is received
for a matching `document` definition, each associated row is completely deleted. If the change
is anything other than a delete event, the rows will be re-created with the new data.
This makes things much easier when trying to deal with multi-level documents (i.e. documents of documents)
and one-to-many table relationships.


## A Couch Tap Project

Couch Tap requires a configuration or filter definition that will allow incoming
document changes to be identified and dealt with.

The following example attempts to outline most of the key features of the DSL.

```ruby
# The couchdb database from which to request the changes feed
changes "http://user:pass@host:port/invoicing" do

  # Which database should we connect to?
  database "postgres://user:pass@localhost:5432/invoicing"

  # Simple automated copy, each property's value in the matching CouchDB
  # document will copied to the table field with the same name.
  document 'type' => 'User' do
    table :users
  end

  document 'type' => 'Invoice' do

    table :invoices, :key => :invoice_id do

      # Copy columns from fields with different name
      column :updated_at, :updated_on
      column :created_at, :created_on

      # Manually set a value from document or fixed variable
      column :date, doc['date'].to_json
      column :added_at, Time.now

      # Set column values from a block.
      column :total do
        doc['items'].inject(0){ |sum,item| sum + item['total'] }
      end

      # Collections perform special synchronization in order to deal with
      # one to one, or indeed many to many relationships.
      #
      # Rather than attempting a complex syncrhonisation process, the current
      # version of Couch Tap will just DELETE all current entries with a
      # primary key id that matches that of the parent table.
      #
      # The foreign id key is assumed to be name of the parent
      # table in singular form with `_id` appended.
      #
      # Each item provided in the array will be made available in the
      # `#data` method, and index from `#index`.
      # `#document` continues to be the complete source document.
      #
      # Collections can be nested to create highly complex structures.
      #
      collection :groups do
        table :invoice_groups do

          collection :entries do
            table :invoice_entries, :key => :entry_id do
              column :date, data['date']
              column :updated_at, document['updated_at']
            end
          end

        end
      end

      # Collections can also be used on Many to Many relationships.
      collection :label_ids do
        table :invoice_labels do
          column :label_id, data
        end
      end

    end

  end
end
```

## DSL Summary

### changes

Defines which CouchDB database should be used to request the changes feed.

After loading the rest of the configuration, the service will
connect to the database using Event Machine. As new changes come into the
system, they will be managed in the background.


### connection

The Sequel URL used to connect to the destination database. Behind the scenes,
Couch Tap will check for a table named `couchdb_sequence` that contains a single
row for the current changes sequence id, much like a migration id typically
seen in a Rails database.

As changes are received from CouchDB, the current sequence will be updated to
match.

#### document

When a document is received from the changes feed, it will be passed through each
`document` stanza looking for a match. Take the following example:

    document :type => 'Invoice' do |doc|
      # ...
    end

This will match all documents whose `type` property is equal to "Invoice". The
document itself will be made available as a hash through the `doc` block variable.

`document` stanzas may be nested if required to provide further levels of
filtering.

#### table

Each `table` stanza lets Couch Tap know that all or part of the current document
should be inserted into it. By default, the matching table's schema will be read
and any field names that match a property in the top-level of the document will
be inserted automatically.

One of the limitations of Couch Tap is that all tables must have an id field as their
primary key. In each row, the id's value will be copied from the `_id` of the
document being imported. This is the only way that deleted documents can be
reliably found and removed from the relational database.

#### column

#### collection

#### foreign_key


### Notes on deleted documents

Synchronising a deleted document is generally a much more complicated operation.
Given that the original document no longer exists in the CouchDB database,
there is no way to know which document group and table the document was inserted
into.

To get around this issue, Couch Tap will search through all the tables defined
for the database and delete rows that match the primary or foreign keys.

Obviously, this is very inefficient. Fortunately, CouchDB is not really suited
to systems that require lots of document deletion, so hopefully this won't be
too much of a problem.


## Testing

Run tests using rake, or individual tests as follows:

    rake test TEST=test/unit/changes_test.rb



