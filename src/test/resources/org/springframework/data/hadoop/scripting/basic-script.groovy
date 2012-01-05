import org.apache.hadoop.fs.*

println "Home dir is " + fs.homeDirectory
println "Work dir is " + fs.workingDirectory
println "/user exists " + fs.exists("/user")

name = UUID.randomUUID().toString()
scriptName = "src/test/resources/test.properties"
fs.copyFromLocalFile(scriptName, name)
println new Path(name).makeQualified(fs)
fs.getLength(name)