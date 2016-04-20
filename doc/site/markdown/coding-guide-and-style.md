# Coding Guide

The basic principle is to always write code that is testable, easy to understand, extensible, resistant to bugs. Code is written once but read by many people so we need to focus also on the next person that will read it:

* Keep it simple
* Test your code
* [Single responsibility principle](http://programmer.97things.oreilly.com/wiki/index.php/The_Single_Responsibility_Principle)
* Try to maintain components [loosely coupled](http://programmer.97things.oreilly.com/wiki/index.php/Cohesion_and_Coupling_matter)
* Do not abuse of comments:
    * Choose names that reveal intent for modules, packages, classes, methods, variables, constants, etc.
    * Do not use comments for excuse bad code or when appropriate naming is enough
    * Comments are good when they provide context (Explain why, not how) or explain things that happen in the real world
* Follow the [Boy Scout Rule](http://programmer.97things.oreilly.com/wiki/index.php/The_Boy_Scout_Rule)
* Don't reinvent the wheel:
    * Use patterns when possible
    * Use proven libraries for (e.g. Apache commons, guava, etc.)
* Refactor when necessary
    * When adding a new feature and the context is not appropriate, refactor first in a separate commit/s
* Use [TestNG](http://testng.org/) for testing instead of JUnit

# Coding Style
Omid coding style should follow general rules found in [Google's Code Style for Java](https://google.github.io/styleguide/javaguide.html), with the following modifications:

* Lines can be up to 120 characters long
* Block indentation should be:
    * 4 spaces
    * Tabs not allowed
* Always use curly braces for code blocks, even for single-line 'ifs' and 'elses'
* Do not include @author tags in any javadoc
* Import ordering and spacing:
    * Try to organize imports alphabetically in blocks with this format:
        * A first block of imports from external libraries
        * A second block of imports from Java libraries
        * Finally a third block with `static` imports
    * Example:
  
```java
    import com.google.common.base.Charsets;
    import com.yahoo.omid.zk.ZKUtils.ZKException;
    import com.yahoo.statemachine.StateMachine.Event;
    import com.yahoo.statemachine.StateMachine.Fsm;
    import com.yahoo.statemachine.StateMachine.State;
    import org.apache.commons.configuration.Configuration;
    import org.slf4j.Logger;
    import org.slf4j.LoggerFactory;
    ...

    import java.io.IOException;
    import java.net.InetSocketAddress;
    import java.util.ArrayDeque;
    ...

    import static com.yahoo.omid.ZKConstants.CURRENT_TSO_PATH;
    import static com.yahoo.omid.zk.ZKUtils.provideZookeeperClient;
    ...
```

The complete rules (specified with [Checkstyle](https://github.com/checkstyle/checkstyle)) can be found in the
`misc/omid_checks.xml` file. A summary of the current status of the style in the Omid project can be found in
the [[checkstyle-aggregate]] page.