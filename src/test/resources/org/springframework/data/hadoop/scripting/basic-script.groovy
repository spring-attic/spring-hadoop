import org.apache.hadoop.fs.*

println "Home dir is " + fs.homeDirectory
println "/user exists " + fs.exists("/user")

name = UUID.randomUUID().toString()
scriptName = "src/test/resources/test.properties"
fs.copyFromLocalFile(scriptName, name)

// use the shell
dir = "script-dir"
if (!fsh.test(dir)) {
   fsh.mkdir(dir); fsh.cp(name, dir); fsh.chmodr(700, dir)
   println "File content is " + fsh.cat(dir + name)
}

println fsh.ls(dir).toString()
fsh.rmr(dir)

fs.getLength(name)