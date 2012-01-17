from java.util import UUID
from org.apache.hadoop.fs import Path

print "Home dir is " + str(fs.homeDirectory)
print "Work dir is " + str(fs.workingDirectory)
print "/user exists " + str(fs.exists("/user"))

name = UUID.randomUUID().toString()
scriptName = "src/test/resources/test.properties"
fs.copyFromLocalFile(scriptName, name)
print Path(name).makeQualified(fs)

# use the shell
dir = "script-dir/"
if not fsh.test(dir):
	fsh.mkdir(dir)
	fsh.cp(name, dir)
	fsh.chmodr(700, dir)
	print "File content is " + str(fsh.cat(dir + name))


print str(fsh.ls(dir))
fsh.rmr(dir)
fs.getLength(name)