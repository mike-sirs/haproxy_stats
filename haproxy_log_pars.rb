require 'mongo'
require 'json'
include Mongo

# arr[4] host name from array

## Connect to mongo DB
db = MongoClient.new("IP_ADDRESS", PORT).db("haproxy")
coll = db.collection("logs")

## Get collection into variable 
coll_log = coll.find().to_a

puts coll.find().count

## Print result
def print_result(host)
  puts "#{host} #{(@host_hash[host] / 1024 / 1024) } Mb, hits: #{hits}"
end

@host_hash = Hash.new(0)
@host_name_arr = Array.new 

traff = 0
hits = 0

coll_log.each do |str| 
  hash = JSON.parse(str.to_json)
  arr = hash["MESSAGE"].split(" ")
  host_name = (arr[4]).gsub(/[.]/, "_").to_sym 		
  @host_hash[host_name] = traff += (arr[0]).to_f 		
  	
  unless @host_name_arr.include?(host_name)
    @host_name_arr.push(host_name)
  end

end 		

@host_name_arr.each { |name| print_result(name) }
	

if coll.find.count >= 1_000_000
        coll.drop
        puts "more then 1000000 rows, droped!"
end
