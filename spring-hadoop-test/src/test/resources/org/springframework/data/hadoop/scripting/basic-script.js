try {load("nashorn:mozilla_compat.js");} catch (e) {} // for Java 8
importPackage(java.util);
importPackage(org.apache.hadoop.fs);

print("Home dir is " + fs.homeDirectory)
print("Work dir is " + fs.workingDirectory)
print("/user exists " + fs.exists("/user"))

name = UUID.randomUUID().toString()
scriptName = "src/test/resources/test.properties"
fs.copyFromLocalFile(scriptName, name)
// use the shell
dir = "script-dir/"
if (!fsh.test(dir)) {
   fsh.mkdir(dir); fsh.cp(name, dir); fsh.chmodr(700, dir)
   print("File content is " + fsh.cat(dir + name))
}

print(fsh.ls(dir))
fsh.rmr(dir)

// return the file length 
fs.getLength(name)