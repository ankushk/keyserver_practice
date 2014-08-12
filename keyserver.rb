require 'rubygems'
require 'sinatra'
require 'data_mapper'
require 'time_difference'

debug = true

# set up in-memory db
DataMapper.setup(:default, 'sqlite::memory:')

# Key class to store in db
class KeyInfo
  include DataMapper::Resource
  property :id, Integer, :key => true
  property :state, String
  property :timestamp, DateTime
end

DataMapper.finalize

KeyInfo.auto_upgrade!

def log_info
  puts "Total records in DB: #{KeyInfo.count}"
  puts "Available keys: #{KeyInfo.count(:state=>"available")}"
  puts "Blocked keys: #{KeyInfo.count(:state=>"blocked")}"
  puts "Purged keys: #{KeyInfo.count(:state=>"purged")}"
end

# Endpoint E1: Generate a random key
get '/generate' do
  puts "Generating random key"
  key = nil
  while key == nil do
    key = rand(1000000)
    puts "Key is #{key}"
    if KeyInfo.get(key)
      puts "Key:#{key} already exists"
      key = nil
    else
      KeyInfo.create(
        :id => key,
        :state => "available",
        :timestamp => Time.now
      )
    end 
  end
  log_info
  "#{key}"
end

# Endpoint E2: Get available key
get '/getkey' do
  puts "Getting a key"
  key = KeyInfo.first(:state => "available")
  if key == nil
    # Show 404
    error 404
  end
  key.update(:state => "blocked", :timestamp => Time.now)
  log_info
  "#{key.id}"
end

# Endpoint E3: Unblock a key
get '/unblock/:key_id' do |key_id|
  puts "Unblocking a key: #{key_id}"
  key = KeyInfo.get(key_id)
  if key == nil
    error 404
  end
  key.update(:state => "available", :timestamp => Time.now)
  log_info
  "#{key.id}"
end

# Endpoint E4: Delete
get '/delete/:key_id' do |key_id|
  puts "Purging the key: #{key_id}"
  key = KeyInfo.get(key_id)
  if key == nil
    error 404
  end
  key.update(:state => "purged", :timestamp => Time.now)
  log_info
  "#{key.id}"
end

# Endpoint E5: Keep alive
get '/keep_alive/:key_id' do |key_id|
  puts "Keep alive for key: #{key_id}"
  key = KeyInfo.get(key_id)
  if key == nil || key.state != "available"
    error 404
  end
  key.update(:timestamp => Time.now)
  log_info
  "#{key.id}"
end

# Updater thread
updater_thr = Thread.new {
  while true do
    KeyInfo.all.each do |key_record|
      puts "KeyRecord Id: #{key_record.id} and state:#{key_record.state}" if debug
      current_time = Time.now
      if key_record.state == "available"
        if TimeDifference.between(key_record.timestamp, current_time).in_seconds >= 300
          puts "Purging the key #{key_record.id}"
          # Purge this key
          key_record.update(:state => "purged")
        end
      elsif key_record.state == "blocked"
        if TimeDifference.between(key_record.timestamp, current_time).in_seconds >= 60
          # Unblock this key
          puts "Updating the key #{key_record.id}"
          key_record.update(:state => "available")
        end
      end
    end
    sleep(1)
  end 
}