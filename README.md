Sample - cics-java-liberty-kafka (Kafka on Liberty with Jakarta EE)

## What this sample does

This sample demonstrates how to integrate **Apache Kafka** with **Jakarta EE** applications running on **WebSphere Liberty**. It focuses on explicit control, minimal dependencies, and clear interaction with Liberty container services.

Unlike framework-heavy approaches, the sample is intentionally transparent: threading, security identity, Kafka polling, and logging behaviour are all visible and controllable by the application.

The sample follows **CICSDev best practices** and is intended both as a runnable example and as an educational reference.

- [cics-java-liberty-kafka](/) - Top-level project.
- [cics-java-liberty-kafka-app](./cics-java-liberty-kafka-app) - Main application project.
- [cics-java-liberty-kafka-bundle](./cics-java-liberty-kafka-bundle) - CICS bundle plug-in based project, contains application and KAFK transaction bundle-parts. Use with Gradle and Maven builds.
- [etc/eclipse_projects/com.ibm.cics.server.examples.liberty.kafka.bundle](./etc/eclipse_projects/com.ibm.cics.server.examples.liberty.kafka.bundle) - CICS Explorer based CICS bundle project, contains application and KAFK transaction bundle-parts. Use with CICS Explorer 'Export to zFS' deployment capability.
- [etc/config/liberty/server.xml](./etc/config/liberty/server.xml) - A template `server.xml` demonstrating the minimum configuration required to run the sample.

---

## Prerequisites

* Java 17 or later on the workstation
   For Java 17+ support, ensure you have:

    * **Gradle**: Version 7.3 or later (recommended: 8.0+)
      - Gradle 7.3+ is required for Java 17 support
      - Gradle 8.x provides better Java 17-21 compatibility
      
    * **Maven**: Version 3.8.1 or later (recommended: 3.9.0+)
      - Maven 3.8.1+ is required for Java 17 support
      - Maven 3.9.x provides improved performance and Java 17+ compatibility

    **Note**: The included Gradle and Maven wrapper scripts are pre-configured with compatible versions.
* WebSphere Liberty
* Apache Kafka broker (local or remote)
* One of the following on your workstation:
  Eclipse with the IBM CICS SDK for Java EE, Jakarta EE and Liberty
  An IDE of your choice that supports Gradle or Maven (or can run the Wrappers)
  A command line, to run the Wrappers or to invoke a locally installed version of Gradle or Maven

---

Downloading
Clone the repository using your IDEs support, such as the Eclipse Git plugin
or, download the sample as a ZIP and unzip onto the workstation
Tip

Eclipse Git provides an 'Import existing Projects' check-box when cloning a repository.

Check dependencies
If you are building this sample with Gradle or Maven you should verify that the correct CICS TS bill of materials (BOM) is specified for your target release of CICS. The BOM specifies a consistent set of artifacts, and adds information about their scope. In the example below the version specified is compatible with CICS TS V6.3, or newer. You can browse the published versions of the CICS BOM at Maven Central.

Gradle (build.gradle):

compileOnly enforcedPlatform("com.ibm.cics:com.ibm.cics.ts.bom:6.3-20250905155520")

Maven (POM.xml):

<dependencyManagement>
    <dependencies>
      <dependency>
        <groupId>com.ibm.cics</groupId>
        <artifactId>com.ibm.cics.ts.bom</artifactId>
        <version>6.3-20250905155520</version>
        <type>pom</type>
        <scope>import</scope>
      </dependency>
    </dependencies>
  </dependencyManagement>

---

## Building the sample

You can build the sample in a variety of ways:

Using the implicit compile/build of the Eclipse based CICS Explorer SDK
Using the built-in Gradle or Maven support of your IDE (For example: buildship or m2e in Eclipse which integrate with the "Run As..." menu.)
Using the supplied Gradle or Maven Wrapper scripts (no requirement for an IDE or Gradle/Maven install)
or you can build it from the command line if you have Gradle or Maven installed on your workstation

Important

The sample comes pre-configured for use with a JDK 17 and CICS TS V6.3 Libraries. When you initially import the project to your IDE, if your IDE is not configured for a JDK 17, or does not have CICS Explorer SDK installed, you might experience local project compile errors. To resolve issues you should configure the Project's build-path to add/remove your preferred combination of CICS TS, JDK, and Liberty's Enterprise Java libraries (Jakarta EE). Resolving errors might also depend on how you wish to build and deploy the sample. If you are building and deploying through CICS Explorer SDK and 'Export to zFS' you should edit the link-app's Project properties. Select 'Java Build Path', on the Libraries tab select 'Classpath', click 'Add Library', select 'CICS with Enterprise Java and Liberty' Library, and choose the appropriate CICS and Enterprise Java versions. If you are building and deploying with Gradle or Maven then you don't necessarily need to fix the local errors, but to do so, you can do as above, or you can run a tooling refresh on the cics-java-liberty-kafka project. For example, in Eclipse: right-click on "Project", select "Gradle -> Refresh Gradle Project", or right-click on "Project", select "Maven -> Update Project...".

Tip

In Eclipse, Gradle (buildship) is able to fully refresh and resolve the local classpath even if the project was previously updated by Maven. However, Maven (m2e) does not currently reciprocate that capability. If you previously refreshed the project with Gradle, you'll need to manually remove the 'Project Dependencies' entry on the Java build-path of your Project Properties to avoid duplication errors when performing a Maven Project Update.

Option 1: Building with Gradle
For a complete build you should run the settings.gradle file in the top-level 'cics-java-liberty-kafka' directory which is designed to invoke the individual build.gradle files for each project.

If successful, a WAR file is created inside the cics-java-liberty-kafka-app/build/libs and  and a CICS bundle ZIP file inside the cics-java-liberty-kafka-bundle/build/distribution directory.

[!NOTE] In Eclipse, the output 'build' directory is often hidden by default. From the Package Explorer panel, select the three dot menu, choose filters and un-check the Gradle build folder to view its contents.

The JVM server the CICS bundle is targeted at is controlled through the cics.jvmserver property, defined in the cics-java-liberty-kafka-bundle/build.gradle file, or alternatively can be set on the command line:

Gradle Wrapper (Linux/Mac):
./gradlew clean build

Gradle Wrapper (Windows):
gradle.bat clean build

Gradle (command-line):
gradle clean build

**Minimum Maven Version**: 3.8.1+ (Java 17 support)
The Maven wrapper included in this project uses Maven 3.9.x, which fully supports Java 17-21.

Option 2: Building with Apache Maven
For a complete build you should run the pom.xml file in the top-level 'cics-java-liberty-kafka' directory. A WAR file is created inside the cics-java-liberty-kafka-app/target directory and a CICS bundle ZIP file inside the cics-java-liberty-kafka-bundle/target directory.

If building a CICS bundle ZIP the CICS JVM server name for the WAR bundle part should be modified in the cics.jvmserver property, defined in cics-java-liberty-link-kafka/pom.xml file under the defaultjvmserver configuration property, or alternatively can be set on the command line.

Maven Wrapper (Linux/Mac):
./mvnw clean verify

Maven Wrapper (Windows):
mvnw.cmd clean verify

Maven (command-line):
mvn clean verify

**Minimum Gradle Version**: 7.3+ (Java 17 support)
The Gradle wrapper included in this project uses Gradle 8.x, which fully supports Java 17-21.

Option 3: Building with Eclipse
If you are using the Egit client to clone the repo, remember to tick the button to import all projects. Otherwise, you should manually Import the projects into CICS Explorer using File → Import → General → Existing projects into workspace, then follow the error resolution advice above.

---

Deploying to a Liberty JVM server

Ensure you have the following features defined in your Liberty server.xml:

<featureManager>
        <feature>appSecurity-5.0</feature>
        <feature>cicsts:core-1.0</feature>
        <feature>microprofile-7.0</feature>
        <feature>transportSecurity-1.0</feature>
        <feature>cicsts:security-1.0</feature>
        <feature>cdi-4.0</feature>
        <feature>restfulWS-3.1</feature>
        <feature>concurrent-3.0</feature>
</featureManager>

Deploying CICS Bundles from Gradle or Maven

Manually upload the ZIP file from the cics-java-liberty-kafka-bundle/target or cics-java-liberty-kafka-bundle/build/distributions directory to zFS.
Unzip this ZIP file on zFS (e.g. ${JAVA_HOME}/bin/jar xf /path/to/bundle.zip).
Create a CICS BUNDLE resource definition, setting the bundle directory attribute to the zFS location you just extracted to, and install it into the CICS region.

Deploying CICS Bundles with CICS Explorer

Optionally, change the name of the JVMSERVER in the .warbundle file of the CICS bundle project to the name of your JVMSERVER resource defined in CICS.
Export the bundle project to zFS by selecting 'Export Bundle project to z/OS Unix File System' from the context menu.
In CICS, create a bundle definition, setting the bundle directory attribute to the zFS location you just exported to, and install it.

Deploying directly with Liberty's application configuration
Manually upload the WAR file from the cics-java-liberty-kafka-app/target or cics-java-liberty-kafka-app/build/libs directory to zFS.
Add an <application> element to the Liberty server.xml to define the web application.

---

## Running the sample

1. Configure the Kafka connection details and credentials as required.
2. Update the Liberty `server.xml` if needed (features, security configuration).
3. Start the Liberty server and deploy the application.

Kafka consumers will start on application-managed background threads during application initialisation.

Make the kafka consumer start/stop using - http://<url>/cics-java-liberty-kafka/control/start?topic=<topic-name> or http://<url>/cics-java-liberty-kafka/control/stop?topic=<topic-name>

---

## Architecture and Design Rationale

This section explains the key architectural decisions made in the sample and the reasons behind them.

### Jakarta EE with Liberty (No `web.xml`)

The application uses **pure Jakarta EE annotations** for:

* Servlet definitions
* Security constraints
* Dependency injection (CDI)

No `web.xml` is required. Liberty natively supports annotation-driven configuration, which:

* Reduces boilerplate
* Improves readability
* Aligns with modern Jakarta EE best practices

Security enforcement is handled by Liberty and Jakarta Security APIs rather than application-managed configuration files.

---

### Kafka Integration via Native Kafka Client APIs

This sample intentionally **does not use MicroProfile Reactive Messaging**.

Instead, it uses the **native Apache Kafka Consumer and Producer APIs**, which:

* Keep data flow explicit
* Avoid additional abstraction layers
* Make threading and offset management easier to reason about
* Minimise dependencies

This approach is particularly useful for customers who need fine-grained control over:

* Poll loops
* Commit strategies
* Error handling
* Security identity propagation

---

### Logging Strategy: Java Util Logging (JUL)

The application uses **Java Util Logging (JUL)** exclusively.

#### Why JUL?

* Built into the JDK (no additional dependencies)
* Integrated with Liberty’s logging infrastructure
* Thread-safe and efficient
* Appropriate for sample code where simplicity and clarity matter

#### Why not `System.out.println` / `System.err.println`?

* Not synchronised
* Poor performance under concurrency
* Bypasses Liberty logging configuration
* Makes diagnostics harder in managed environments

#### Why not Log4j / SLF4J / Logback?

* Introduces unnecessary dependencies for a sample
* Adds logging bridge and classloader complexity
* Distracts from Liberty’s native logging model

#### Important note on output

By default, JUL output appears in messages.log when the console log level permits.

Ensure the following JVM option is set:
  -Dcom.ibm.ws.logging.console.log.level=INFO

This is documented intentionally so users understand Liberty’s logging behaviour.

---

### Threading, Identity, and Context Propagation

Kafka consumers run on **application-managed background threads**.

Because these threads are not container-managed request threads, the application must explicitly handle:

* Security identity
* Thread-local context
* Integration with Liberty security services

Key points:

* **ThreadLocals are used deliberately**, despite their cost, to associate security identity and contextual data with long-lived Kafka consumer threads
* Usage is tightly scoped and clearly documented in code
* This reflects real-world constraints when integrating asynchronous processing with container-managed security

---

### Credential and State Management

The sample uses in-memory data structures such as **HashMaps** to manage:

* Topic-specific credentials
* Consumer state
* Runtime flags

This choice:

* Keeps the sample easy to understand
* Avoids persistence or external configuration complexity
* Makes runtime behaviour visible and debuggable

All such usage is documented inline, including limitations and trade-offs.

---

## Security Model Demonstrated

This sample demonstrates explicit, application-managed security identity propagation for Kafka consumer threads running in WebSphere Liberty.
Kafka consumers execute on application-managed background threads, so security identity must be established explicitly rather than being inherited from a container-managed request thread.

Two supported approaches are documented:

Option A: Subject-based RunAs identity (default)
Option B: authData-based identity (alternative)

Only one option should be used at a time.

Option A: Subject-Based RunAs Identity (Default)

This is the default approach implemented in the sample code. The application explicitly establishes a RunAs identity on Kafka consumer threads using Liberty security APIs.

Key characteristics:

A Subject representing the service identity is obtained during application initialisation
The identity is applied using WSSubject.setRunAsSubject(subject)
The RunAs subject is set once per consumer thread and reused for subsequent processing
Previous identity state is captured and managed explicitly

This approach:

* Works well with long-lived Kafka consumer threads
* Provides full control over credentials per topic
* Supports **“service ID per topic”** models
* Makes identity propagation explicit and observable

Option B: authData-Based Identity (Alternative)

As an alternative to Subject-based RunAs identity, the sample can be configured to use Liberty authData to associate credentials with outbound Kafka connections.

In this model:

Service credentials are defined declaratively in server.xml
The application references the configured authData by name
Liberty manages credential lookup and association
No explicit Subject or RunAs switching is performed in application code

This approach:

Centralises credential management in server configuration
Reduces application-level security handling
Is preferred when credentials must not appear in application code
Aligns well with operationally managed environments

When Option B is used, the Subject-based RunAs logic should be disabled or bypassed to avoid conflicting identity models.

server.xml configuration (Option B only)

Option B only: authData-based Kafka credentials        


<server>
  <featureManager>
    <feature>appSecurity-5.0</feature>
    <feature>cdi-4.0</feature>
    <feature>servlet-6.0</feature>
    <feature>concurrent-3.0</feature>
    <feature>passwordUtilities-1.0</feature>
    <feature>zosPasswordEncryptionKey-1.0</feature>
  </featureManager>

  <!-- Obtain AES key from RACF key ring at runtime -->
  <zosPasswordEncryptionKey
      keyring="safkeyring:///YOUR.KEYRING"
      type="JCERACFKS"
      label="Liberty"/>
      
      
  <!-- (Optional) Use RACF key ring as SSL keystore -->
  <keyStore id="defaultKeyStore"
            fileBased="false"
            type="JCERACFKS"
            location="safkeyring:///YOUR.KEYRING"
            password="password"/>

  <!-- Credentials alias (AES-protected) -->
  <authData id="cics_Auth_Id" user="<user_id>" password="{aes}..."/>

</server>

```
-  Create the RACF key ring and certificates (label = `Liberty`)

> Run these TSO commands with appropriate IDs and DNs for your environment.
> YOUR.KEYRING is the generic value for keyring.
> Not sure about the syntax of these commands I iterated through some and had failures
> and can't remember the winning combo

```tso
/* Create key ring (owned by Liberty STC user, e.g., <user_id>) */
RACDCERT ADDRING(YOUR.KEYRING) ID(<user_id>)                   

/* (Optional) Create a CERTAUTH root CA */
RACDCERT GENCERT CERTAUTH +
  SUBJECTSDN( CN('MyLibertyCA') C('UK') O('YourOrg') OU('Liberty') ) +
  WITHLABEL('LIBERTY.CA') NOTAFTER(DATE(2030/12/31))

/* Create a personal certificate labeled 'Liberty' for <user_id> */
RACDCERT GENCERT ID(<user_id>) +
  SUBJECTSDN( CN('liberty.example.com') C('UK') O('YourOrg') OU('Liberty') ) +
  WITHLABEL('Liberty') +
  SIGNWITH(CERTAUTH LABEL('LIBERTY.CA')) +
  RSA SIZE(2048) NOTAFTER(DATE(2028/12/31))

/* Connect CA + personal cert to the key ring */
RACDCERT ID(<user_id>) CONNECT(CERTAUTH LABEL('LIBERTY.CA') RING(YOUR.KEYRING))
RACDCERT ID(<user_id>) CONNECT(ID(<user_id>) LABEL('Liberty') RING(YOUR.KEYRING) USAGE(PERSONAL) DEFAULT)

/* Verify ring contents */
RACDCERT LISTRING(YOUR.KEYRING) ID(<user_id>)
                                                                          
    Digital ring information for user IN00501:                                  
                                                                                
      Ring:                                                                     
          >YOUR.KEYRING<                                                      
      Certificate Label Name             Cert Owner     USAGE      DEFAULT      
      --------------------------------   ------------   --------   -------      
      LIBERTY.CA                            CERTAUTH    CERTAUTH     NO         
      Liberty                            ID(<user_id>)    PERSONAL     YES 

```

Reference: JCERACFKS/JCECCARACFKS examples and ring setup [IBM Docs](https://www.ibm.com/docs/en/was-liberty/nd?topic=ssl-configuring-keyring-based-keystore).

Generate the AES‑encoded password with `securityUtility` (key from RACF)

From `${wlp.install.dir}/wlp/bin`, or from SSH shell on zFS to the same, run:

```bash
securityUtility encode \
  --encoding=aes \
  --keyring=safkeyring:///YOUR.KEYRING \
  --keyringType=JCERACFKS \
  --keyLabel=Liberty \
  "YourRacfPassword"

```

<!-- Do not configure authData when using Option A -->

Kafka client configuration should reference the above authData entry when this option is selected.

---

## Repository Structure

```
├── src/
│   └── main/
│       ├── java/
│       │   └── com.example.kafkaliberty
│       └── resources/
│           └── META-INF/
├── server/
│   └── server.xml
├── .github/
│   └── workflows/
├── README.md
└── build.gradle / pom.xml
```

The structure follows established **CICSDev Samples** conventions to ensure consistency and ease of reuse.

---

## Intended Audience

This sample is intended for:

* Customers integrating Kafka with Liberty and Jakarta EE
* Developers requiring explicit control over threading and security
* Architects comparing Jakarta EE and Spring Boot approaches

---

Find out more
For more information about invoking Java EE applications in a Liberty JVM server from CICS programs, see Linking to Java applications in a Liberty JVM server by using the @CICSProgram annotation.

License
This project is licensed under Eclipse Public License - v 2.0.

Usage terms
By downloading, installing, and/or using this sample, you acknowledge that separate license terms may apply to any dependencies that might be required as part of the installation and/or execution and/or automated build of the sample, including the following IBM license terms for relevant IBM components:

• IBM CICS development components terms: https://www.ibm.com/support/customer/csol/terms/?id=L-ACRR-BBZLGX
