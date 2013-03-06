
# Couch Tap

Utility to listen to a CouchDB changes feed and automatically insert, update,
or delete rows matching document types into a relational database.

While CouchDB is awesome for developers, business people probably won't be
quite as impressed when they want to play around with the data. Regular SQL
is simply much easier to use and much more widely supported by a larger
range of comercial tools.

Couch Tap will listen to incoming documents on a CouchDB's changes
stream and automatically update rows of RDBMS tables defined in the
conversion schema. The changes stream uses a sequence number allowing
synchronisation to be started and stopped at will.

Ruby's fast and simple sequel library is used to provide the connection to the
database. This library can also be used for migrations.


## A Couch Tap Project

Couch Tap requires a configuration or filter definition that will allow incoming
document changes to be identified and dealt with.


    # The couchdb database from which to request the changes feed
    changes "http://user:pass@host:port/invoicing" do

      # Which database should we connect to?
      database "postgres://user:pass@localhost:5432/invoicing"

      # Simple automated copy, each property's value in the matching CouchDB
      # document will copied to the table field with the same name.
      filter 'type' => 'User' do
        table :users
      end

      filter 'type' => 'Invoice' do

        table :invoices do
          # Which field should be used for the primary key?
          primary_key :id, :code

          # Copy columns from fields with different name
          column :updated_at, :updated_on
          column :created_at, :created_on

          # Manually set a value from document or fixed variable
          column :date, document['date']
          column :added_at, Time.now

          # Set column values from a block
          column :total do
            doc['items'].inject(0){ |sum,item| sum + item['total'] }
          end
        end

        # Collections perform special synchronization.
        # If there is no primary key for the table, each row will be deleted
        # and recreated an on update. This might be very inefficient.
        collection :items do |item|
          table :invoice_items do
            foreign_key :invoice_id, doc['_id']
          end
        end

      end
    end

## Stanza Summary

### changes

Defines which CouchDB database should be used to request the changes feed. The server
should respond to the standard.


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



#### primary_key

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



