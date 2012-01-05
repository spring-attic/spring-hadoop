require 'java'
include org.apache.hadoop.fs

puts "Home dir is " + $fs.homeDirectory.to_s
puts "Work dir is " + $fs.workingDirectory.to_s
puts "/user exists " + $fs.exists("/user").to_s

name = java.util.UUID.randomUUID().to_s
scriptName = "src/test/resources/test.properties"
$fs.copyFromLocalFile(scriptName, name)
puts Path.new(name).makeQualified($fs)
# return the file length 
$fs.getLength(name)