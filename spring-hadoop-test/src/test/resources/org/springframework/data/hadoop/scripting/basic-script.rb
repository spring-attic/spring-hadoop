require 'java'
include org.apache.hadoop.fs

puts "Home dir is " + $fs.homeDirectory.to_s
puts "Work dir is " + $fs.workingDirectory.to_s
puts "/user exists " + $fs.exists("/user").to_s

name = java.util.UUID.randomUUID().to_s
scriptName = "src/test/resources/test.properties"
$fs.copyFromLocalFile(scriptName, name)
puts Path.new(name).makeQualified($fs)

# use the shell
dir = "script-dir/"
if !$fsh.test(dir)
	$fsh.mkdir(dir)
	$fsh.cp(name, dir)
	$fsh.chmodr(700, dir)
	print "File content is " + $fsh.cat(dir + name).to_s
end

print $fsh.ls(dir).to_s
$fsh.rmr(dir)

# return the file length
$fs.getLength(name)