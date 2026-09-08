# cics-java-liberty-kafka
[![Build](https://github.com/cicsdev/cics-java-liberty-kafka/actions/workflows/build.yaml/badge.svg?branch=main)](https://github.com/cicsdev/cics-java-liberty-kafka/actions/workflows/build.yaml)
[![License](https://img.shields.io/badge/License-EPL%202.0-green.svg)](https://www.eclipse.org/legal/epl-2.0/)

## Overview

This sample demonstrates how to integrate **Apache Kafka** with **IBM CICS** using **Jakarta EE** and **WebSphere Liberty**. Unlike framework-heavy approaches, this implementation uses native Kafka Consumer APIs for explicit control over threading, security, and message processing.

The sample is intended both as a runnable example and as an educational reference for developers building enterprise-grade Kafka consumers with Jakarta EE.

**Key Features:**
- Consumes messages from multiple Kafka topics using native Kafka Consumer APIs
- Processes each message within a CICS transaction context
- Demonstrates security identity propagation in Liberty using Jakarta EE
- Shows how to map different topics to different CICS transaction IDs
- Provides two alternative security approaches for credential management

**Key Differences from the companion Spring Boot Kafka sample:**
- Uses native Apache Kafka Consumer APIs instead of `@KafkaListener`
- Application-managed consumer threads with explicit lifecycle control
- MicroProfile Config instead of Spring's `application.properties`
- Jakarta EE CDI and JAX-RS instead of Spring annotations

---

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Design and Architecture](#design-and-architecture)
4. [How It Works](#how-it-works)
5. [Security Models Explained](#security-models-explained)
6. [Before You Start: Files to Modify](#before-you-start-files-to-modify)
7. [Project Structure](#project-structure)
8. [Thread Pool Management and TCLASS Considerations](#thread-pool-management-and-tclass-considerations)
9. [Downloading](#downloading)
10. [Building the Sample](#building-the-sample)
11. [Deploying to a CICS Liberty JVM server](#deploying-to-a-cics-liberty-jvm-server)
12. [Running the Sample](#running-the-sample)
13. [Troubleshooting](#troubleshooting)
14. [Logging Strategy](#logging-strategy)
15. [License](#license)
16. [Additional Resources](#additional-resources)
17. [Contributing](#contributing)

---

## Prerequisites

- CICS TS V6.1 or later with a configured Liberty JVM server
- APAR PH70996 must be applied to the CICS region
- Java SE 17 or later on the workstation
- Eclipse with the IBM CICS SDK for Java EE, Jakarta EE and Liberty (optional)
- Gradle or Apache Maven on the workstation (optional — wrappers are supplied)
- Network connectivity from z/OS to your Kafka broker(s)

---

## Design and Architecture

### High-Level Design Intent

This sample addresses a fundamental challenge: **how to safely consume Kafka messages within CICS transactions while maintaining security context using Jakarta EE**.

Key components:

1. **Native Kafka Consumer APIs** — Explicit control over polling, threading, and offset management
2. **Liberty ManagedExecutorService** — Ensures worker threads are CICS-aware
3. **Explicit Security Context Propagation** — Captures and applies security identity using WSSubject
4. **Per-Topic Transaction Mapping** — Routes messages to appropriate CICS transactions based on topic
5. **Controlled Lifecycle Management** — Allows dynamic start/stop of topic consumers via REST endpoints

### Architecture Diagram

**Alternative A: Subject-Based RunAs Identity (Default)**

```
┌─────────────────────────────────────────────────────────────────┐
│                         Kafka Cluster                           │
│                    (topics: orders, test-topic)                 │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             │ Messages
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                    CICS Liberty JVM Server                      │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │           Jakarta EE Application (WAR)                    │  │
│  │                                                           │  │
│  │  ┌──────────────────────────────────────────────────┐     │  │
│  │  │         KafkaController (REST Endpoint)          │     │  │
│  │  │  • /control/start?topic=xxx                      │     │  │
│  │  │  • /control/stop?topic=xxx                       │     │  │
│  │  │  • Captures caller's Subject (HTTP auth)         │     │  │
│  │  └────────────┬─────────────────────────────────────┘     │  │
│  │               │                                           │  │
│  │               ▼                                           │  │
│  │  ┌──────────────────────────────────────────────────┐     │  │
│  │  │      KafkaConsumerService                        │     │  │
│  │  │  • Creates consumer thread per topic             │     │  │
│  │  │  • Native Kafka Consumer API polling             │     │  │
│  │  │  • Sets RunAs Subject on consumer thread         │     │  │
│  │  └────────────┬─────────────────────────────────────┘     │  │
│  │               │                                           │  │
│  │               ▼                                           │  │
│  │  ┌──────────────────────────────────────────────────┐     │  │
│  │  │      KafkaMessageProcessor                       │     │  │
│  │  │  • Submits to ManagedExecutorService             │     │  │
│  │  │  • Wraps in CICSTransactionRunnable              │     │  │
│  │  └────────────┬─────────────────────────────────────┘     │  │
│  │               │                                           │  │
│  │               ▼                                           │  │
│  │  ┌──────────────────────────────────────────────────┐     │  │
│  │  │   Liberty ManagedExecutorService                 │     │  │
│  │  │  • CICS-aware thread pool                        │     │  │
│  │  │  • Inherits RunAs Subject                        │     │  │
│  │  └────────────┬─────────────────────────────────────┘     │  │
│  │               │                                           │  │
│  │               ▼                                           │  │
│  │  ┌──────────────────────────────────────────────────┐     │  │
│  │  │   CICSTransactionRunnable.run()                  │     │  │
│  │  │  • Executes under CICS transaction               │     │  │
│  │  │  • Transaction ID from KafkaConfig               │     │  │
│  │  │  • Processes message business logic              │     │  │
│  │  └──────────────────────────────────────────────────┘     │  │
│  │                                                           │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

**Alternative B: authData-Based Identity with Programmatic Login**

The architecture is identical to Alternative A, with one key difference in the KafkaController:

```
┌──────────────────────────────────────────────────┐
│         KafkaController (REST Endpoint)          │
│  • /control/start?topic=xxx                      │
│  • /control/stop?topic=xxx                       │
│  • Uses LoginManager.getSubject()                │  ← Different from Alternative A
│    (programmatic login with authData)            │
└──────────────────────────────────────────────────┘
         │
         ▼
    (rest of flow identical to Alternative A)
```

**Key Difference:**
- **Alternative A:** Subject from authenticated HTTP request (`WSSubject.getCallerSubject()`)
- **Alternative B:** Subject from programmatic login (`LoginManager.getSubject()` using server.xml authData)
- **Both:** Use the same RunAs mechanism (`WSSubject.setRunAsSubject(subject)`)

See [Security Models Explained](#security-models-explained) for detailed comparison.

### Component Responsibilities

| Component | Purpose | Key Features |
|-----------|---------|--------------|
| **RestApplication** | Jakarta EE REST application entry point | `@ApplicationPath("/")` |
| **KafkaController** | REST API for life-cycle control | `/start`, `/stop`, captures Subject |
| **KafkaConsumerService** | Per-topic Kafka consumer management | Native Kafka Consumer API, sets RunAs Subject |
| **KafkaMessageProcessor** | Async message processing | Submits to ManagedExecutorService |
| **KafkaConfig** | Configuration and topic-to-transaction mapping | MicroProfile Config, `getTranIdForTopic()` |
| **LoginManager** | Alternative: Subject via programmatic login | JAAS login using authData (optional) |

---

## How It Works

### Message Flow (Step-by-Step)

1. **User Initiates Consumer**
   - HTTP GET/POST to `/control/start?topic=orders`
   - KafkaController obtains a Subject (security identity):
     - **Alternative A (default):** Captures caller's Subject from HTTP request
     - **Alternative B:** Uses LoginManager to get Subject from authData
   - Subject is stored in a map keyed by topic name
   - New consumer thread is created and started for that topic

2. **Kafka Consumer Polls for Messages**
   - Consumer thread runs a poll loop using native Kafka Consumer API
   - `consumer.poll(Duration.ofMillis(200))` retrieves batches of messages
   - Each message is processed individually

3. **Security Context is Applied**
   - The consumer thread calls `WSSubject.setRunAsSubject(subject)`
   - This establishes the security identity for all subsequent work
   - Liberty's ManagedExecutorService will inherit this identity

4. **Message Processing is Offloaded**
   - Each message is wrapped in a `CICSTransactionRunnable`
   - The runnable is submitted to Liberty's `ManagedExecutorService`
   - This ensures the processing happens on a CICS-aware thread

5. **CICS Transaction Executes**
   - The `CICSTransactionRunnable.run()` method executes
   - The transaction ID is determined by `getTranid()`, which looks up the topic in KafkaConfig
   - Business logic processes the message (in this sample, just logging)

6. **User Stops Consumer**
   - HTTP GET/POST to `/control/stop?topic=orders`
   - Consumer thread is signaled to stop
   - `consumer.wakeup()` interrupts any in-progress poll
   - Subject mapping is removed from the map

### Multi-Topic Approach

This sample demonstrates **per-topic consumers with individual life-cycle control**.

**Why separate consumers per topic?**
- Each topic can be started/stopped independently
- Different topics can use different CICS transaction IDs
- Allows per-topic security contexts (different users for different topics)
- Simplifies monitoring and troubleshooting

**Transaction ID Mapping:**
The `microprofile-config.properties` file maps topics to transaction IDs:
```properties
cics.transaction.map.test-topic=KAFK
cics.transaction.map.orders=KAF1
```

If no mapping exists, the default transaction ID `CJSU` is used.

---

## Security Models Explained

This sample provides **two alternative approaches** for managing security credentials. Choose the one that best fits your operational requirements.

### Alternative A: Subject-Based RunAs Identity (Default - Implemented)

**How it works:**
1. User authenticates to Liberty (HTTP Basic, JWT, OIDC, etc.)
2. `/control/start` captures the caller's Subject via `WSSubject.getCallerSubject()`
3. Consumer thread sets this Subject as its RunAs identity
4. ManagedExecutorService inherits the identity
5. CICS transactions run under this identity

**Configuration:**
- No special server.xml configuration needed beyond basic security
- Security is established via the HTTP request
- Each topic can have a different identity (different users call `/start`)

**Pros:**
- Simple configuration
- Flexible per-topic security
- No credential storage in configuration files

**Code Location:**
- `KafkaController.start()` — captures Subject
- `KafkaConsumerService.handleMessage()` — sets RunAs Subject

---

### Alternative B: authData-Based Identity with Programmatic Login (Alternative - Not Active by Default)

**How it works:**
1. Credentials are stored in Liberty's `server.xml` as `<authData>`
2. Password is AES-encrypted using a key from a RACF keyring
3. Application performs **programmatic JAAS login** using these credentials via `LoginManager`
4. Resulting **Subject** is obtained and used the same way as Alternative A
5. This Subject is then set as RunAs identity on consumer threads (same mechanism as Alternative A)

**Configuration Required:**

1. **Create the RACF key ring and certificates**

Run these TSO commands with appropriate IDs and DNs for your environment.
`YOUR.KEYRING` is the generic placeholder for your keyring name.

```tso
/* Create key ring (owned by Liberty STC user, e.g., <user_id>) */
RACDCERT ADDRING(YOUR.KEYRING) ID(<user_id>)

/* (Optional) Create a CERTAUTH root CA */
RACDCERT GENCERT CERTAUTH +
  SUBJECTSDN(CN('MyLibertyCA') C('UK') O('YourOrg') OU('Liberty')) +
  WITHLABEL('LIBERTY.CA') NOTAFTER(DATE(2030/12/31))

/* Create a personal certificate labeled 'Liberty' for <user_id> */
RACDCERT GENCERT ID(<user_id>) +
  SUBJECTSDN(CN('liberty.example.com') C('UK') O('YourOrg') OU('Liberty')) +
  WITHLABEL('Liberty') +
  SIGNWITH(CERTAUTH LABEL('LIBERTY.CA')) +
  RSA SIZE(2048) NOTAFTER(DATE(2028/12/31))

/* Connect CA + personal cert to the key ring */
RACDCERT ID(<user_id>) CONNECT(CERTAUTH LABEL('LIBERTY.CA') RING(YOUR.KEYRING))
RACDCERT ID(<user_id>) CONNECT(ID(<user_id>) LABEL('Liberty') RING(YOUR.KEYRING) USAGE(PERSONAL) DEFAULT)

/* Verify ring contents */
RACDCERT LISTRING(YOUR.KEYRING) ID(<user_id>)
```

**Expected output:**
```
Digital ring information for user <user_id>:

  Ring:
      >YOUR.KEYRING<
  Certificate Label Name             Cert Owner     USAGE      DEFAULT
  --------------------------------   ------------   --------   -------
  LIBERTY.CA                            CERTAUTH    CERTAUTH     NO
  Liberty                            ID(<user_id>)  PERSONAL     YES
```

**Reference:** [IBM Docs - JCERACFKS/JCECCARACFKS keyring setup](https://www.ibm.com/docs/en/was-liberty/nd?topic=ssl-configuring-keyring-based-keystore)

2. **Enable features in server.xml:**
```xml
<feature>passwordUtilities-1.0</feature>
<feature>zosPasswordEncryptionKey-1.0</feature>
```

3. **Configure RACF keyring in server.xml:**
```xml
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
```

4. **Generate AES-encoded password with `securityUtility` (key from RACF)**

From `${wlp.install.dir}/wlp/bin`, or from SSH shell on zFS:

```bash
securityUtility encode \
  --encoding=aes \
  --keyring=safkeyring:///YOUR.KEYRING \
  --keyringType=JCERACFKS \
  --keyLabel=Liberty \
  "YourRacfPassword"
```

This will output something like:
```
{aes}ARI673meZr9vyGHN8xKJdLx9...
```

5. **Define authData in server.xml:**
```xml
<!-- Credentials alias (AES-protected) -->
<authData id="cicsSAF" user="<user_id>" password="{aes}ARI673meZr9vy...."/>
```

6. **Activate LoginManager in code:**
Uncomment in `KafkaController.java`:
```java
@Inject
private LoginManager loginManager;
```

Then use in `start()` method:
```java
Subject subject = loginManager.getSubject();
```

**Pros:**
- Credentials managed centrally in server.xml
- No clear-text passwords in configuration
- Suitable for automated/service accounts
- Aligns with enterprise security policies

**Cons:**
- More complex setup (RACF keyring required)
- Requires z/OS security administrator involvement

**Code Location:**
- `LoginManager.java` — performs programmatic login
- `KafkaController.start()` — would use LoginManager instead of WSSubject.getCallerSubject()

---

## Before You Start: Files to Modify

Before building and deploying this sample, you **must** customize the following files with your environment-specific values:

### 1. Kafka Connection Configuration
**File:** `cics-java-liberty-kafka-app/src/main/resources/META-INF/microprofile-config.properties`

**What to change:**
```properties
bootstrap.servers=<YOUR_KAFKA_BROKER_IP>:9092

test-topic.key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
test-topic.value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
test-topic.group.id=test-group

cics.transaction.map.<your-topic>=<YOUR_TRANID>
```

**Example:**
```properties
bootstrap.servers=9.109.246.51:9092

test-topic.key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
test-topic.value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
test-topic.group.id=test-group

cics.transaction.map.test-topic=KAFK
cics.transaction.map.orders=KAF1
```

---

### 2. Liberty Server Configuration
**File:** `etc/config/liberty/server.xml`

**For Alternative A (Subject-based — default):**
`cicsts:security-1.0` is auto-added by CICS when `SEC=YES` in SIT. The required features are:

```xml
<featureManager>
    <feature>microprofile-7.0</feature>
    <feature>transportSecurity-1.0</feature>
    <feature>concurrent-3.0</feature>
</featureManager>
```

**For Alternative B (authData-based):**
See detailed setup instructions in [Security Models Explained — Alternative B](#alternative-b-authdata-based-identity-with-programmatic-login-alternative---not-active-by-default).
Requires: RACF keyring, AES-encrypted password, authData configuration.

---

### 3. Build Configuration (Optional)
**Files:**
- `cics-java-liberty-kafka-cicsbundle/build.gradle`
- `cics-java-liberty-kafka-cicsbundle/pom.xml`

Only needed when using **CICS Bundle Plugin Deployment**. Set your target JVM server name on the command line:

Gradle:
```shell
./gradlew clean build "-Pcics.jvmserver=MYJVM"
```

Maven:
```shell
./mvnw clean verify "-Dcics.jvmserver=MYJVM"
```

---

### 4. Java Code (Only for Alternative B)

**Required changes:**
- Uncomment `LoginManager` injection in `KafkaController.java`
- Update `AUTH_DATA_ID` in `LoginManager.java` to match your server.xml

See detailed instructions in [Security Models Explained — Alternative B](#alternative-b-authdata-based-identity-with-programmatic-login-alternative---not-active-by-default).

---

### Summary Checklist

Before building:
- [ ] Updated `microprofile-config.properties` with Kafka broker address and topic properties
- [ ] Configured topic-to-transaction mappings in `microprofile-config.properties`
- [ ] Chose security Alternative (A or B)
- [ ] If Alternative A: verified `server.xml` has required features
- [ ] If Alternative B: created RACF keyring and generated AES password
- [ ] If Alternative B: updated `server.xml` with features, keyring and authData
- [ ] If Alternative B: uncommented LoginManager in `KafkaController.java`
- [ ] If using CICS Bundle Plugin Deployment: set JVM server name via `-Pcics.jvmserver` / `-Dcics.jvmserver`

---

## Project Structure

```
cics-java-liberty-kafka/
├── README.md                                            # This file
├── LICENSE                                              # EPL 2.0 license
├── build.gradle                                         # Root Gradle build
├── settings.gradle                                      # Gradle multi-project settings
├── pom.xml                                              # Root Maven POM
├── gradlew / gradlew.bat                                # Gradle wrapper scripts
├── mvnw / mvnw.cmd                                      # Maven wrapper scripts
│
├── cics-java-liberty-kafka-app/                         # Main application
│   ├── build.gradle                                     # App-level Gradle build
│   ├── pom.xml                                          # App-level Maven POM
│   └── src/main/
│       ├── java/com/ibm/cicsdev/kafka/
│       │   ├── RestApplication.java                     # Jakarta EE REST application
│       │   ├── KafkaController.java                     # REST API for start/stop
│       │   ├── KafkaConsumerService.java                # Native Kafka consumer management
│       │   ├── KafkaMessageProcessor.java               # Async message processing
│       │   ├── KafkaConfig.java                         # Topic-to-transaction mapping
│       │   └── LoginManager.java                        # Optional: authData login
│       ├── resources/META-INF/
│       │   └── microprofile-config.properties           # Kafka & MicroProfile config
│       └── webapp/WEB-INF/
│           ├── beans.xml                                # CDI configuration
│           ├── ibm-web-ext.xml                          # Liberty context-root
│           └── web.xml                                  # Jakarta EE 10 web descriptor
│
├── cics-java-liberty-kafka-cicsbundle/                  # CICS bundle (Gradle/Maven)
│   ├── build.gradle                                     # Bundle Gradle build
│   ├── pom.xml                                          # Bundle Maven POM
│   └── src/main/bundleParts/
│       └── KAFK.transaction                             # CICS transaction definition
│
├── cics-java-liberty-kafka-cicsbundle-eclipse/          # Eclipse CICS bundle project
│   ├── META-INF/cics.xml                                # Bundle manifest
│   └── cics-java-liberty-kafka.warbundle               # WAR bundle part descriptor
│
└── etc/config/liberty/
    └── server.xml                                       # Liberty server template
```

---

## Thread Pool Management and TCLASS Considerations

### Overview

In CICS Liberty environments with multiple applications sharing a single JVM server, proper thread pool management is critical to prevent application starvation and deadlocks. This section explains the challenges and solutions implemented in this sample.

---

### The Problem: Thread Starvation in Multi-Application Environments

**Architecture Context:**
- Multiple web applications run inside a single CICS Liberty JVM server
- Each application may have its own endpoint/virtualhost
- CICS regions are cloned for scalability/resilience
- Sysplex distributor load-balances between applications across cloned regions

**The Challenge:**

In traditional CICS, you use **TCLASS** to throttle applications and prevent any one application from consuming too much resource. However, in Liberty, this approach can cause **deadlocks**:

1. **HTTP requests enter Liberty** and are allocated a thread from the shared pool
2. **Only then** does the CICS interceptor check TCLASS limits
3. **If TCLASS limit is reached**, the requesting thread is blocked
4. **The blocked thread is NOT released** back to the thread pool
5. **Result**: The entire server can deadlock, starving other applications

**Why This Matters:**

- Liberty in CICS has a **maximum thread limit of 256**
- Application starvation can occur at lower request rates than expected
- One misbehaving application can impact all applications in the JVM server

---

### The Solution: Custom Managed Executors

This sample implements **custom ManagedExecutorService** with application-specific thread limits to prevent starvation.

#### How It Works

Instead of using Liberty's default executor (shared by all applications), each application defines its own executor with specific concurrency limits:

```xml
<!-- In server.xml -->
<managedExecutorService jndiName="concurrent/KafkaExecutor"
     contextServiceRef="DefaultContextService"
     maxThreads="10"
     coreThreads="5"/>
```

**Benefits:**
- ✅ Kafka application cannot consume more than `maxThreads`
- ✅ Other applications in the same JVM server are protected
- ✅ Prevents deadlock scenarios with TCLASS
- ✅ Provides predictable resource allocation

#### Implementation in Code

The `KafkaMessageProcessor` injects the custom executor:

```java
@Resource(lookup = "concurrent/KafkaExecutor")
private ManagedExecutorService executor;
```

When Kafka messages arrive, they are submitted to this bounded executor:

```java
executor.submit(new KafkaCICSTransactionRunnable(topic, message));
```

If the executor's queue is full, new submissions will block or be rejected (depending on policy), preventing unbounded thread consumption.

---

### Configuration Guidelines

#### Determining Thread Limits

Consider these factors when setting `maxThreads`:

1. **Expected Message Rate**: How many messages per second?
2. **Processing Time**: How long does each message take to process?
3. **Other Applications**: How many other apps share this JVM server?
4. **Total Thread Budget**: CICS Liberty max is 256 threads

#### Recommended Configuration

**For Low-Volume Applications (< 100 msg/sec):**
```xml
<managedExecutorService jndiName="concurrent/KafkaExecutor"
     contextServiceRef="DefaultContextService"
     maxThreads="10"
     coreThreads="3"/>
```

**For Medium-Volume Applications (100-500 msg/sec):**
```xml
<managedExecutorService jndiName="concurrent/KafkaExecutor"
     contextServiceRef="DefaultContextService"
     maxThreads="20"
     coreThreads="5"/>
```

**For High-Volume Applications (> 500 msg/sec):**
```xml
<managedExecutorService jndiName="concurrent/KafkaExecutor"
     contextServiceRef="DefaultContextService"
     maxThreads="40"
     coreThreads="10"/>
```

---

## Downloading

**If using Eclipse:** the simplest approach is to clone the repository using the Eclipse Git plugin (EGit) perspective.

**If using the command line:**
```shell
git clone https://github.com/cicsdev/cics-java-liberty-kafka
```
Alternatively, download the sample as a [ZIP](https://github.com/cicsdev/cics-java-liberty-kafka/archive/main.zip) and unzip onto the workstation.

**If importing into Eclipse:**
1. In the **Git Repositories** view, right-click the repository → **Import as Project** (imports the root project)
   *(if you cloned from the command line, use **File → Import → Existing Projects into Workspace** instead, browse to the cloned directory, select all projects, and skip to step 5)*
2. Switch to the **Java EE** perspective
3. In the **Project Explorer**, right-click the `cics-java-liberty-kafka-app` folder → **Import as Project**
4. Right-click the `cics-java-liberty-kafka-cicsbundle` folder → **Import as Project**
5. Right-click the `cics-java-liberty-kafka-cicsbundle-eclipse` folder → **Import as Project**
6. **Required:** Right-click the root project → **Gradle → Refresh Gradle Project** or **Maven → Update Project...** — this resolves CICS and MicroProfile dependencies into the project classpath. Without this step, the WTP export will produce an incomplete WAR missing `WEB-INF/lib`.

### Check dependencies

Before building this sample, verify that the correct CICS TS bill of materials (BOM) is specified for your target release of CICS. The BOM specifies a consistent set of artifacts and their scopes. The default version in this sample is compatible with CICS TS V6.1 with JCICS APAR PH63856 or newer — update it to match your CICS TS release.

You can browse the published versions of the CICS BOM at [Maven Central](https://mvnrepository.com/artifact/com.ibm.cics/com.ibm.cics.ts.bom).

Gradle (`cics-java-liberty-kafka-app/build.gradle`):

`compileOnly(enforcedPlatform("com.ibm.cics:com.ibm.cics.ts.bom:6.1-20250812133513-PH63856"))`

Maven (`cics-java-liberty-kafka-app/pom.xml`):

```xml
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>com.ibm.cics</groupId>
      <artifactId>com.ibm.cics.ts.bom</artifactId>
      <version>6.1-20250812133513-PH63856</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>
```

---

## Building the Sample

You can build the sample using an IDE of your choice, or from the command line. Using the supplied Gradle or Maven wrapper is the recommended approach to get a consistent build tool version.

The required build tasks are `clean build` for Gradle and `clean verify` for Maven. Gradle generates a WAR file in `cics-java-liberty-kafka-app/build/libs`; Maven generates it in `cics-java-liberty-kafka-app/target`.

### Gradle Wrapper (command line)

On Linux or Mac:

```shell
./gradlew clean build
```

On Windows:

```shell
gradlew.bat clean build
```

This creates a WAR file inside the `cics-java-liberty-kafka-app/build/libs` directory and a CICS bundle ZIP inside `cics-java-liberty-kafka-cicsbundle/build/distributions`.

> **Note:** In Eclipse, the `build` directory may be hidden by default. To view it: **Package Explorer → ⋮ → Filters and Customization → uncheck "Gradle build folder"**. For Maven, the `target` directory is visible by default.

### Maven Wrapper (command line)

On Linux or Mac:

```shell
./mvnw clean verify
```

On Windows:

```shell
mvnw.cmd clean verify
```

This creates a WAR file inside the `cics-java-liberty-kafka-app/target` directory and a CICS bundle ZIP inside `cics-java-liberty-kafka-cicsbundle/target`.

### Building with Eclipse (IDE)

Once imported (see [Downloading](#downloading)), use the IDE's built-in Gradle or Maven integration:

**With Gradle (Buildship):**
1. Right-click the root project → **Run As → Gradle Build...**
2. Enter `clean build` in the Gradle Tasks field → **Run**
3. After the build completes, right-click the root project → **Gradle → Refresh Gradle Project**

**With Maven (m2e):**
1. Right-click the root project → **Maven → Update Project...** → check **Force Update of Snapshots/Releases** → **OK**
2. Right-click the root project → **Run As → Maven build...** → enter `clean verify` → **Run**

---

## Deploying to a CICS Liberty JVM server

Ensure the following features are defined in your Liberty `server.xml`:

```xml
<featureManager>
    <feature>microprofile-7.0</feature>
    <feature>transportSecurity-1.0</feature>
    <feature>concurrent-3.0</feature>
</featureManager>
```

> **Note:** `cicsts:security-1.0` is auto-injected by CICS when `SEC=YES` in SIT — do not add it manually.

A template `server.xml` is provided [here](./etc/config/liberty/server.xml).

### CICS Bundle Plugin Deployment (Gradle/Maven)

This method uses the `cics-bundle-gradle-plugin` or `cics-bundle-maven-plugin` to automatically generate a CICS bundle.

**Deploy the bundle:**

1. Build the project (see [Building the Sample](#building-the-sample)) — the CICS bundle ZIP is produced automatically
2. Upload the bundle ZIP to zFS:
   - Gradle: `cics-java-liberty-kafka-cicsbundle/build/distributions/cics-java-liberty-kafka-cicsbundle-1.0.0.zip`
   - Maven: `cics-java-liberty-kafka-cicsbundle/target/cics-java-liberty-kafka-cicsbundle-1.0.0.zip`
3. On z/OS, extract the bundle:
   ```shell
   jar xf cics-java-liberty-kafka-cicsbundle-1.0.0.zip
   ```
4. Create and install a CICS BUNDLE resource definition:
   ```
   CEDA DEFINE BUNDLE(KAFKBNDL) GROUP(MYGROUP) BUNDLEDIR(/path/to/cics-java-liberty-kafka-cicsbundle-1.0.0)
   CEDA INSTALL BUNDLE(KAFKBNDL) GROUP(MYGROUP)
   ```

### CICS Explorer SDK Deployment

This repository includes a pre-configured Eclipse CICS bundle project `cics-java-liberty-kafka-cicsbundle-eclipse`.

1. Right-click `cics-java-liberty-kafka-cicsbundle-eclipse` → **Export Bundle Project to z/OS UNIX File System** and follow the wizard

> **Note:** The bundle project is pre-configured so that the Eclipse WTP export automatically packages the application WAR with all dependencies. This relies on the `-app` project being open in the same Eclipse workspace. If you have not yet imported it, follow the import steps in [Downloading](#downloading) first.

### Direct Liberty Application Deployment

1. Build the WAR using Gradle or Maven (see [Building the Sample](#building-the-sample))
2. Upload the WAR file to zFS
3. Add an `<application>` element to your Liberty `server.xml`:

```xml
<application id="cics-java-liberty-kafka"
    location="${server.config.dir}/apps/cics-java-liberty-kafka.war"
    name="cics-java-liberty-kafka" type="war">
    <application-bnd>
        <security-role name="cicsAllAuthenticated">
            <special-subject type="ALL_AUTHENTICATED_USERS"/>
        </security-role>
    </application-bnd>
</application>
```

> **Note:** The `id`, `location`, and `name` values use the repository name, not a versioned artifact name. This matches the context root set in `ibm-web-ext.xml` and ensures a consistent URL regardless of build tool or version.

---

## Running the Sample

### Step 1: Verify Deployment

Check Liberty `messages.log` for successful application start:
```
CWWKT0016I: Web application available (default_host): http://hostname:9080/cics-java-liberty-kafka/
```

---

### Step 2: Start a Kafka Consumer

Using curl with authentication (Alternative A):
```bash
curl -u userid:password "http://hostname:9080/cics-java-liberty-kafka/control/start?topic=test-topic"
```

Using a browser (will prompt for credentials):
```
http://hostname:9080/cics-java-liberty-kafka/control/start?topic=test-topic
```

**Expected response:**
```
Started listener for topic=test-topic
```

---

### Step 3: Verify Message Processing

Produce a message to Kafka from your broker:
```bash
kafka-console-producer --broker-list localhost:9092 --topic test-topic
```

Then check Liberty `messages.log`:
```
[INFO] Received message from topic test-topic having message : Hello from Kafka!
[INFO] Task USERID = YOURUSERID
[INFO] DEBUG: Finished processing Kafka message in thread: ...
```

---

### Step 4: Stop the Consumer

```bash
curl -u userid:password "http://hostname:9080/cics-java-liberty-kafka/control/stop?topic=test-topic"
```

**Expected response:**
```
Stopped listener for topic=test-topic
```

---

### Multiple Topics

You can run multiple consumers simultaneously. Each topic runs independently with its own consumer thread.

Start consumer for test-topic:
```bash
curl -u userid:password "http://hostname:9080/cics-java-liberty-kafka/control/start?topic=test-topic"
```

Start consumer for orders topic:
```bash
curl -u userid:password "http://hostname:9080/cics-java-liberty-kafka/control/start?topic=orders"
```

---

## Troubleshooting

### Issue: "Failed to set RunAsSubject"

**Symptom:**
```
SEVERE: Failed to set RunAsSubject for topic 'test-topic'
```

**Cause:** Liberty security features not properly configured

**Solution:**
1. Verify `cicsts:security-1.0` feature is present (auto-injected when `SEC=YES` in SIT)
2. Ensure user is authenticated before calling `/start`
3. Check that `SEC=YES` in CICS SIT parameters

---

### Issue: AES password decryption fails (Alternative B)

**Symptom:**
```
RuntimeException: Programmatic login failed for authData alias 'cicsSAF'
```

**Cause:** RACF keyring or AES password configuration issue

**Solution:**
1. Verify RACF keyring exists: `RACDCERT LISTRING(YOUR.KEYRING) ID(<user_id>)`
2. Verify certificate label matches: `Liberty`
3. Regenerate AES password with correct keyring parameters
4. Ensure `zosPasswordEncryptionKey` in server.xml matches keyring configuration

---

### Issue: "No Kafka properties found for topic"

**Symptom:**
```
WARNING: No Kafka properties found for topic: test-topic
```

**Cause:** Missing or incorrect configuration in `microprofile-config.properties`

**Solution:**
1. Verify `bootstrap.servers` is set
2. Verify topic-specific properties exist (e.g., `test-topic.key.deserializer`)
3. Check file location: `cics-java-liberty-kafka-app/src/main/resources/META-INF/microprofile-config.properties`
4. Rebuild and redeploy the application

---

### Logging and Diagnostics

Enable detailed logging in `server.xml`:
```xml
<logging traceSpecification="*=info:com.ibm.cicsdev.kafka.*=all"/>
```

Key log locations:
- Liberty: `${wlp.user.dir}/servers/<server>/logs/messages.log`
- CICS: MSGUSR, CSSL transient data queues
- Kafka: Check broker logs if messages are not being produced

---

## Logging Strategy

This sample uses **Java Util Logging (JUL)** for simplicity and integration with Liberty.

**Why JUL?**
- Built into JDK (no dependencies)
- Integrates with Liberty's logging infrastructure
- Thread-safe and efficient

**Why not System.out.println?**
- Not thread-safe
- Poor performance under concurrency
- Bypasses Liberty logging configuration

**Why not Log4j/SLF4J?**
- Adds unnecessary dependencies for a sample
- Introduces classloader complexity
- JUL is sufficient for this use case

By default, JUL output appears in `messages.log`. Ensure this JVM option is set in your JVM profile:
```
-Dcom.ibm.ws.logging.console.log.level=INFO
```

---

## License

This project is licensed under [Eclipse Public License - v 2.0](LICENSE).

By downloading, installing, and/or using this sample, you acknowledge that separate license terms may apply to any dependencies required for installation, execution, or automated builds, including IBM CICS development components: https://www.ibm.com/support/customer/csol/terms/?id=L-ACRR-BBZLGX

---

## Additional Resources

- [CICS Liberty Documentation](https://www.ibm.com/docs/en/cics-ts/latest?topic=liberty-cics)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Jakarta EE Documentation](https://jakarta.ee/)
- [MicroProfile Documentation](https://microprofile.io/)
- [WebSphere Liberty Documentation](https://www.ibm.com/docs/en/was-liberty)

---

## Contributing

This sample is maintained by IBM CICS development. We welcome bug reports and feature requests via GitHub Issues. Contributions are welcome and reviewed on a case-by-case basis — please read the [contributing guidelines](https://github.com/cicsdev/.github/blob/main/CONTRIBUTING.md) before opening a pull request. For CICS product questions, contact IBM Support.
