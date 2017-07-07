# Couch Tap [![Build Status](https://travis-ci.org/cabify/couch_tap.svg?branch=master)](https://travis-ci.org/cabify/couch_tap)

Utility to listen to a CouchDB changes feed and automatically insert, update,
or delete rows into a relational database from matching key-value conditions of incoming documents.

While CouchDB is awesome, business people probably won't be
quite as impressed when they want to play around with the data. Regular SQL
is generally accepted as being easy to use and much more widely supported by a larger
range of comercial tools.

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

## Couch Tap architecture

### Publisher/Subscriber model

CouchTap gem internally uses a queue to decouple the listening to CouchDB changes bit from the persisting to the
relational system bit. We can say we have two publishers and one consumer from the internal queue.

* Consumer:
  * `QueryExecutor`: This class buffers whatever operations appear in the queue and, once `batch_size` is reached, it runs all buffered
    operations against the relational db in a single transaction.
* Publishers:
  * `Changes`: Listens for changes from CouchDB's feed and publishes as many operations required for that change to get reflected in the DB.
    The operations published will depend on the configuration given from the DSL.
  * `Timer`: Publishes a special operation into the queue to signal that `timeout` seconds have elapsed. This is to avoid records getting delayed
    if the changes feed takes a while to fill the `QueryExecutor`'s buffer.

## A Couch Tap Project

Couch Tap requires a configuration or filter definition that will allow incoming
document changes to be identified and dealt with.

The following example attempts to outline most of the key features of the DSL.

```ruby
# The couchdb database from which to request the changes feed and the max time we want our records to wait in the queue
changes couch_db: "http://user:pass@host:port/invoicing", timeout: 60 do

  # Which database should we connect to?
  # How big do you want your batches to be?
  database db: "postgres://user:pass@localhost:5432/invoicing", batch_size: 10_000

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

Hash with two keys:

* `:couch_db`: Defines which CouchDB database should be used to request the changes feed.
* `:timeout`: Defines the timeout the `Timer` will have. It represents the maximum delay you're happy to accept since a change is received from CouchDB until it appears in your DB

After loading the rest of the configuration, the service will
connect to the database using Event Machine. As new changes come into the
system, they will be managed in the background.

### database

Hash with two keys:

* `:db`: The Sequel URL used to connect to the destination database.
* `:batch_size`: The number of operations you want to buffer together before sending them to the database.

Behind the scenes, Couch Tap will check for a table named `couchdb_sequence` that contains a single
row for the current changes sequence id and the last timestamp when something was written, much like a migration id typically
seen in a Rails database.

As changes are received from CouchDB, the current sequence will be updated to
match along with the `last_transaction_at` that will contain either `Time.now` or the maximum `updated_at` inserted value.

`last_transaction_at`'s reason is to let you know how far behind CouchTap is in synchronsing changes.

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

## Metrics

CouchTap code is instrumented to report basic monitoring metrics to DataDog via `Metrics` class. Specifically reported metrics are:

* `documents_parsed`: Rate of the documents parsed (received from the feed) per time unit.
* `transactions`: Rate of the transactions run against the database per time unit.
* `queue.back_pressure`: Number of operations standing in the queue (waiting to be processed) when a transaction is ran.
* `delete.time`: Latency histogram of the delete time tagged by target database table name.
* `delete.latency.unit`: Avg time taken to delete a single record within a whole transaction. Tagged by target database table name.
* `insert.time`: Latency histogram of the insert time tagged by target database table name.
* `insert.latency.unit`: Avg time taken to insert a single record within a whole transaction. Tagged by target database table name.
* `transactions.time`: Latency histogram of the whole transaction time.
* `insertions`: Rate of records written to the database per time unit. Tagged by target database table name.
* `delay`: How desynchronised the database is in seconds. It is based on the `updated_at` value found on inserted records.

