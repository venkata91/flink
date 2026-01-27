# Next Steps: Verify Flink 1.18 + Auron Integration

## ✅ What's Complete

You successfully switched from Flink master (2.3-SNAPSHOT) to Flink 1.18 to match Auron's dependencies!

**Branch:** `auron-integration` (based on `release-1.18`)
**Status:** All code ported, compiled, and installed
**JARs:** Ready in Maven local repository

## 🎯 What You Need to Do Now

### Step 1: Rebuild Auron Against Flink 1.18 (with Auron Integration)

The Flink 1.18 JARs with Auron integration are now in your Maven local repository. Rebuild Auron to pick them up:

```bash
cd /Users/vsowrira/git/auron

# Full rebuild (recommended for first time)
./auron-build.sh --pre --sparkver 3.5 --scalaver 2.12 --flinkver 1.18 --clean true

# Or faster build (skip test compilation and execution)
./auron-build.sh --pre --sparkver 3.5 --scalaver 2.12 --flinkver 1.18 -Dmaven.test.skip=true
```

**What this does:**
- Pulls Flink 1.18 dependencies from Maven local (includes AuronExecNodeGraphProcessor, AuronBatchExecNode)
- Compiles AuronExecNodeConverter and AuronTransformationFactory
- Creates `auron-flink-planner-7.0.0-SNAPSHOT.jar`

**Expected output:**
```
[INFO] BUILD SUCCESS
```

### Step 2: Run Verification Test

This test compares execution plans with Auron enabled vs disabled:

```bash
cd /Users/vsowrira/git/auron/auron-flink-extension/auron-flink-planner

# Run verification test
mvn test -Dtest=AuronAutoConversionVerificationTest
```

**What to look for:**

✅ **SUCCESS - Conversion Working:**
```
==================== PLAN WITHOUT AURON ====================
Calc(select=[id, product], where=[>(amount, 100)])
+- TableSourceScan(...)

==================== PLAN WITH AURON ====================
AuronBatchExecNode[Calc(select=[id, product], where=[>(amount, 100)])
  +- TableSourceScan(...)]

✅ Plans are different - good sign!
✅ SUCCESS: Auron appears in execution plan!
   Automatic conversion is WORKING - queries will use native execution
```

❌ **FAILURE - Not Converting:**
```
⚠️  WARNING: Plans are identical!
   This means Auron conversion may not be happening.
```

If you see failure, check:
1. Did Auron rebuild pick up Flink 1.18 JARs? Check `mvn dependency:tree`
2. Are test classpath issues? Try `mvn clean test`
3. Check logs for ClassNotFoundException or other errors

### Step 3: Verify JAR Contents (Optional)

Double-check that everything is in place:

```bash
cd /Users/vsowrira/git/auron

# Check Auron converter classes are in JAR
jar tf auron-flink-extension/auron-flink-planner/target/auron-flink-planner-7.0.0-SNAPSHOT.jar | grep -E "(AuronExecNodeConverter|AuronTransformationFactory)"
# Should show both classes

# Check Auron depends on Flink 1.18
cd auron-flink-extension/auron-flink-planner
mvn dependency:tree | grep "flink-table-planner"
# Should show: org.apache.flink:flink-table-planner_2.12:jar:1.18-SNAPSHOT
```

## 📊 Expected Result

If everything works:

1. ✅ Auron builds successfully against Flink 1.18
2. ✅ Verification test shows plans are DIFFERENT
3. ✅ "AuronBatchExecNode" or "Auron" appears in execution plan
4. ✅ Automatic conversion is WORKING!

## 🐛 Troubleshooting

### Problem: "Plans are identical"

**Likely cause:** Auron didn't pick up new Flink 1.18 JARs

**Solution:**
```bash
cd /Users/vsowrira/git/auron

# Force clean rebuild
./auron-build.sh --pre --sparkver 3.5 --scalaver 2.12 --flinkver 1.18 --clean true

# Check Maven picked up correct version
cd auron-flink-extension/auron-flink-planner
mvn dependency:list | grep flink-table-planner
# Should show: flink-table-planner_2.12:jar:1.18-SNAPSHOT
```

### Problem: ClassNotFoundException

**Symptom:**
```
ClassNotFoundException: org.apache.flink.table.planner.plan.nodes.exec.processor.AuronExecNodeGraphProcessor
```

**Likely cause:** Flink 1.18 JARs not in Maven local or Auron using wrong version

**Solution:**
```bash
# 1. Verify Flink JARs are in Maven local
ls -la ~/.m2/repository/org/apache/flink/flink-table-planner_2.12/1.18-SNAPSHOT/
# Should see: flink-table-planner_2.12-1.18-SNAPSHOT.jar

# 2. Verify JAR contains Auron classes
jar tf ~/.m2/repository/org/apache/flink/flink-table-planner_2.12/1.18-SNAPSHOT/flink-table-planner_2.12-1.18-SNAPSHOT.jar | grep -i auron
# Should show: AuronExecNodeGraphProcessor.class and AuronBatchExecNode.class

# 3. If missing, rebuild Flink
cd /Users/vsowrira/git/flink
git checkout auron-integration
./mvnw clean install -DskipTests -pl flink-table/flink-table-api-java,flink-table/flink-table-planner -am
```

### Problem: Auron build fails with dependency errors

**Symptom:**
```
[ERROR] Failed to execute goal on project auron-flink-planner: Could not resolve dependencies
```

**Solution:**
```bash
# Clear Maven cache and rebuild
rm -rf ~/.m2/repository/org/apache/flink/flink-table-*

# Rebuild Flink 1.18
cd /Users/vsowrira/git/flink
git checkout auron-integration
./mvnw clean install -DskipTests -pl flink-table/flink-table-api-java,flink-table/flink-table-planner -am

# Then rebuild Auron
cd /Users/vsowrira/git/auron
./auron-build.sh --pre --sparkver 3.5 --scalaver 2.12 --flinkver 1.18 --clean true
```

## 📁 Where Everything Is

### Flink Side (Version 1.18)

**Repository:** `/Users/vsowrira/git/flink`
**Branch:** `auron-integration` (based on `release-1.18`)

**Modified/Added Files:**
1. `flink-table/flink-table-api-java/.../OptimizerConfigOptions.java` - Config flag
2. `flink-table/flink-table-planner/.../AuronExecNodeGraphProcessor.java` - Pattern detector (NEW)
3. `flink-table/flink-table-planner/.../AuronBatchExecNode.java` - Wrapper node (NEW)
4. `flink-table/flink-table-planner/.../BatchPlanner.scala` - Processor registration

**Built JARs:**
- `flink-table/flink-table-api-java/target/flink-table-api-java-1.18-SNAPSHOT.jar`
- `flink-table/flink-table-planner/target/flink-table-planner_2.12-1.18-SNAPSHOT.jar`

**Installed To:**
- `~/.m2/repository/org/apache/flink/flink-table-api-java/1.18-SNAPSHOT/`
- `~/.m2/repository/org/apache/flink/flink-table-planner_2.12/1.18-SNAPSHOT/`

### Auron Side (Targets Flink 1.18)

**Repository:** `/Users/vsowrira/git/auron`
**Branch:** `main` (or whatever branch you're on)

**Files:**
1. `auron-flink-extension/auron-flink-planner/.../AuronExecNodeConverter.java` - Converts ExecNodes to Auron plans
2. `auron-flink-extension/auron-flink-planner/.../AuronTransformationFactory.java` - Creates Flink transformations

**Will Build:**
- `auron-flink-extension/auron-flink-planner/target/auron-flink-planner-7.0.0-SNAPSHOT.jar`

### Documentation (All in /Users/vsowrira/git/auron/)

- **README_FLINK_INTEGRATION.md** - Quick start and overview
- **FLINK_INTEGRATION.md** - Architecture and design
- **HOW_TO_VERIFY.md** - Step-by-step verification guide
- **VERIFICATION_GUIDE.md** - 6 verification methods
- **INTEGRATION_STATUS.md** - Implementation status
- **AURON_INTEGRATION_1.18.md** - This port to 1.18 (in Flink repo)

## 🚀 After Verification Succeeds

Once you see "🎉 AUTOMATIC CONVERSION IS WORKING!", you can:

1. **Test with Real Queries**
   ```java
   Configuration config = new Configuration();
   config.setBoolean("table.optimizer.auron.enabled", true);
   config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);

   StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
   StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

   // Create Parquet table
   tEnv.executeSql(
       "CREATE TABLE sales (" +
       "  id BIGINT, product STRING, amount DOUBLE" +
       ") WITH (" +
       "  'connector' = 'filesystem'," +
       "  'path' = 'file:///path/to/sales.parquet'," +
       "  'format' = 'parquet'" +
       ")"
   );

   // Run query - automatically uses Auron!
   tEnv.executeSql("SELECT id, product FROM sales WHERE amount > 100").print();
   ```

2. **Measure Performance**
   - Compare execution time with Auron ON vs OFF
   - Expected: 2-10x speedup depending on dataset size

3. **Extend to More Patterns**
   - Add support for aggregations (GROUP BY, SUM, COUNT)
   - Add support for joins
   - Add support for sorting (ORDER BY)

## 📝 Summary

**What Changed:**
- ✅ Switched Flink from master (2.3-SNAPSHOT) to release-1.18
- ✅ Ported all Auron integration code to Flink 1.18
- ✅ Built and installed Flink 1.18 JARs with Auron classes
- ✅ Version aligned: Both Flink and Auron on 1.18

**What's Next:**
1. Rebuild Auron against Flink 1.18
2. Run verification test
3. Confirm automatic conversion is working

**Expected Time:**
- Auron rebuild: ~5-10 minutes
- Verification test: ~1 minute
- **Total: ~10 minutes**

---

**Ready?** Start with Step 1: Rebuild Auron

```bash
cd /Users/vsowrira/git/auron
./auron-build.sh --pre --sparkver 3.5 --scalaver 2.12 --flinkver 1.18 --clean true
```

Good luck! 🚀
