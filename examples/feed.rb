
# Sample Configuration Script
#
# Run using the command line application:
#
#     couch_tap feed.rb
#


changes "http://user:pass@host:port/invoicing" do

  # Which database should we connect to?
  database "sqlite:///database.sqlite3"

  filter 'type' => 'User' do
    table :users
  end

  filter 'type' => 'Journey' do
    table :journeys
  end


end



