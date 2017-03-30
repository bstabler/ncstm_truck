# ncstm_truck
Long and short distance truck model used in North Carolina Statewide Model.

This project includes the following packages:

  - `MPOTrucks`: Disaggregation of statewide flows to MPO regions; used in
  Metrolina (Charlotte) and the Piedmont Triad (Greensboro).
  - `national`: long distance models based on Freight Analysis Framework
  - `statewide`: short-distance truck models
  - `r3logit`: long-distance mode choice model
  
This project carries an open-source MIT license. See [`LICENSE`](LICENSE) 
for details.

## Compiling
The source code can be compiled using any java compiler, though build scripts
for [Apache Ant](http://ant.apache.org) are provided in the project. 

  1. Install a java developer's kit (JDK) for Java 1.8+. 
  Instructions to do this are available from [Oracle](https://docs.oracle.com/javase/8/docs/technotes/guides/install/install_overview.html).
  2. Set your `JAVA_HOME` system environment variable to point to the JDK you
  installed.
  3. Install [Apache Ant](http://ant.apache.org).
  4. Make sure the `ant` command is available on your command line.
  5. Clone this repository (`git clone https://github.com/pbsag/ncstm_truck`).
  6. Open a command window pointed at the cloned working copy.
  7. Run `ant all` to compile the module and create `ncstm.jar`, which
  will be located in the `release/` directory.
  
``` bash  
[ncstm_truck]$ ant all
Buildfile: /Users/Greg/code/projects/ncstm_truck/build.xml

init:
     [echo] ***** JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_121.jdk/Contents/Home

clean.module.ncstm:
   [delete] Deleting directory /Users/Greg/code/projects/ncstm_truck/build/classes

clean:

compile.module.ncstm:
    [mkdir] Created dir: /Users/Greg/code/projects/ncstm_truck/build/classes
    [javac] /Users/Greg/code/projects/ncstm_truck/module_ncstm.xml:37: warning: 'includeantruntime' was not set, defaulting to build.sysclasspath=last; set to false for repeatable builds
    [javac] Compiling 15 source files to /Users/Greg/code/projects/ncstm_truck/build/classes

compile:

makejar:
      [jar] Building jar: /Users/Greg/code/projects/ncstm_truck/release/ncstm.jar

release:

all:

BUILD SUCCESSFUL
Total time: 8 seconds 
```
