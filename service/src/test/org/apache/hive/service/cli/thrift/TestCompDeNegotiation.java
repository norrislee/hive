/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.service.cli.thrift;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.service.auth.HiveAuthConstants;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.thrift.EmbeddedThriftBinaryCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIService;
import org.apache.hive.service.rpc.thrift.TOpenSessionReq;
import org.apache.hive.service.rpc.thrift.TOpenSessionResp;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;

public class TestCompDeNegotiation {
  private HiveConf noCompDes;
  private HiveConf serverSingleCompDe;
  private HiveConf serverMultiCompDes1;
  private HiveConf serverMultiCompDes2;
  private HiveConf clientSingleCompDe;
  private HiveConf clientMultiCompDes1;
  private HiveConf clientMultiCompDes2;
  private HiveConf serverCompDeConf;
  private HiveConf clientCompDeConf;

  @Before
  public void init() throws Exception {
    HiveConf baseConf = new HiveConf();
    baseConf.setVar(ConfVars.HIVE_SERVER2_AUTHENTICATION, HiveAuthConstants.AuthTypes.NONE.toString());
    baseConf.setVar(ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider");
    baseConf.setBoolean("datanucleus.schema.autoCreateTables", true);

    // Set the compressor lists and compressor configs used for negotiation

    noCompDes = new HiveConf(baseConf);

    clientSingleCompDe = new HiveConf(baseConf);
    clientSingleCompDe.set(clientCompressorListVarName(), "compde3");
    clientSingleCompDe.set(clientCompdeParamPrefix("compde3") + ".version", "1.0");
    serverSingleCompDe = new HiveConf(baseConf);
    serverSingleCompDe.setVar(ConfVars.HIVE_SERVER2_THRIFT_RESULTSET_COMPRESSOR_LIST, "compde3");

    clientMultiCompDes1 = new HiveConf(baseConf);
    clientMultiCompDes1.set(clientCompressorListVarName(), "compde1,compde2,compde3,compde4");
    clientMultiCompDes1.set(clientCompdeParamPrefix("compde1") + ".version", "1.0");
    clientMultiCompDes1.set(clientCompdeParamPrefix("compde2") + ".version", "1.0");
    clientMultiCompDes1.set(clientCompdeParamPrefix("compde3") + ".version", "1.0");
    clientMultiCompDes1.set(clientCompdeParamPrefix("compde4") + ".version", "1.0");
    serverMultiCompDes1 = new HiveConf(baseConf);
    serverMultiCompDes1.setVar(ConfVars.HIVE_SERVER2_THRIFT_RESULTSET_COMPRESSOR_LIST, "compde1,compde2,compde3,compde4");

    clientMultiCompDes2 = new HiveConf(baseConf);
    clientMultiCompDes2.set(clientCompressorListVarName(), "compde2, compde4");
    clientMultiCompDes2.set(clientCompdeParamPrefix("compde2") + ".version", "2.0");
    clientMultiCompDes2.set(clientCompdeParamPrefix("compde4") + ".version", "1.0");
    serverMultiCompDes2 = new HiveConf(baseConf);
    serverMultiCompDes2.setVar(ConfVars.HIVE_SERVER2_THRIFT_RESULTSET_COMPRESSOR_LIST, "compde2, compde4");

    serverCompDeConf = new HiveConf(baseConf);
    serverCompDeConf.setVar(ConfVars.HIVE_SERVER2_THRIFT_RESULTSET_COMPRESSOR_LIST, "compde3");
    serverCompDeConf.set(noCompDeConfigPrefix("compde3-2.0") + ".test1", "serverVal1");
    serverCompDeConf.set(noCompDeConfigPrefix("compde3-2.0") + ".test2", "serverVal2");//overriden by client
    serverCompDeConf.set(noCompDeConfigPrefix("compde3-2.0") + ".test4", "serverVal4");//overriden by plug-in
    serverCompDeConf.set(noCompDeConfigPrefix("compde3-1.0") + ".test5", "serverVal5");//no used

    clientCompDeConf = new HiveConf(baseConf);
    clientCompDeConf.set(clientCompressorListVarName(), "compde3");
    clientCompDeConf.set(clientCompdeParamPrefix("compde3-2.0") + ".version", "1.0,2.0");
    clientCompDeConf.set(clientCompdeParamPrefix("compde3-2.0") + ".test2", "clientVal2");//overrides server
    clientCompDeConf.set(clientCompdeParamPrefix("compde3-2.0") + ".test3", "clientVal3");
    clientCompDeConf.set(clientCompdeParamPrefix("compde3-2.0") + ".test5", "clientVal5");//overriden by plug-in
    clientCompDeConf.set(clientCompdeParamPrefix("compde3-1.0") + ".test6", "clientVal6");//not used
  }

  private static String noCompDeConfigPrefix(String compDeName) {
    return ConfVars.HIVE_SERVER2_THRIFT_RESULTSET_COMPRESSOR.varname + "." + compDeName;
  }
  // The JDBC driver prefixes all configuration names before sending the request and the server expects these prefixes
  private static String clientCompressorListVarName() {
    return "set:hiveconf:" + ConfVars.HIVE_SERVER2_THRIFT_RESULTSET_COMPRESSOR_LIST.varname;
  }
  private static String clientCompdeParamPrefix(String compDeName) {
    return "set:hiveconf:" + ConfVars.HIVE_SERVER2_THRIFT_RESULTSET_COMPRESSOR.varname + "." + compDeName;
  }

  public class MockServiceWithoutCompDes extends EmbeddedThriftBinaryCLIService {
    @Override
    // Pretend that we have no CompDe plug-ins
    protected Map<String, String> initCompde(
        String compdeName,
        String compdeVersion,
        HiveConf serverConf,
        HiveConf clientConf) throws Exception {
      throw new Exception("No supported compdes");
    }
  }

  @Test
  // The server has no CompDe plug-ins
  public void testServerWithoutCompDePlugins() throws HiveSQLException, InterruptedException, TException {
    ThriftCLIService service = new MockServiceWithoutCompDes();
    service.init(noCompDes);

    TOpenSessionReq req = new TOpenSessionReq();
    req.setConfiguration(new HashMap<String, String>());
    TOpenSessionResp resp;

    req.setConfiguration(noCompDes.getValByRegex(".*"));
    resp = service.OpenSession(req);
    assertNull(resp.getCompressorName());

    req.setConfiguration(clientSingleCompDe.getValByRegex(".*"));
    resp = service.OpenSession(req);
    assertNull(resp.getCompressorName());

    req.setConfiguration(clientMultiCompDes2.getValByRegex(".*"));
    resp = service.OpenSession(req);
    assertNull(resp.getCompressorName());

    service.stop();
  }

  public class MockServiceWithCompDes extends EmbeddedThriftBinaryCLIService {
    @Override
    // Pretend that we have plug-ins for all CompDes except "compde1"
    protected Map<String, String> initCompde(
        String compdeName,
        String compdeVersion,
        HiveConf serverConf,
        HiveConf clientConf) throws Exception {
      if (compdeName.equals("compde1")) {
        throw new Exception("compde1 not supported");
      }
      else {
        return serverConf.getValByRegex(".*");
      }
    }
  }

  @Test
  // The server has plug-ins but the CompDe list is not configured
  public void testServerWithoutCompDeInList() throws HiveSQLException, InterruptedException, TException {
    ThriftCLIService service = new MockServiceWithCompDes();
    service.init(noCompDes);

    TOpenSessionReq req = new TOpenSessionReq();
    req.setConfiguration(new HashMap<String, String>());
    TOpenSessionResp resp;

    req.setConfiguration(noCompDes.getValByRegex(".*"));
    resp = service.OpenSession(req);
    assertNull(resp.getCompressorName());
    assertNull(resp.getCompressorVersion());

    req.setConfiguration(clientSingleCompDe.getValByRegex(".*"));
    resp = service.OpenSession(req);
    assertNull(resp.getCompressorName());
    assertNull(resp.getCompressorVersion());

    req.setConfiguration(clientMultiCompDes2.getValByRegex(".*"));
    resp = service.OpenSession(req);
    assertNull(resp.getCompressorName());
    assertNull(resp.getCompressorVersion());

    service.stop();
  }

  @Test
  public void testServerWithSingleCompDeInList() throws HiveSQLException, InterruptedException, TException {
    ThriftCLIService service = new MockServiceWithCompDes();
    service.init(serverSingleCompDe);

    TOpenSessionReq req = new TOpenSessionReq();
    req.setConfiguration(new HashMap<String, String>());
    TOpenSessionResp resp;

    req.setConfiguration(noCompDes.getValByRegex(".*"));
    resp = service.OpenSession(req);
    assertNull(resp.getCompressorName());
    assertNull(resp.getCompressorVersion());

    req.setConfiguration(clientSingleCompDe.getValByRegex(".*"));
    resp = service.OpenSession(req);
    assertEquals("compde3", resp.getCompressorName());
    assertEquals("1.0", resp.getCompressorVersion());

    req.setConfiguration(clientMultiCompDes2.getValByRegex(".*"));
    resp = service.OpenSession(req);
    assertNull(resp.getCompressorName());
    assertNull(resp.getCompressorVersion());

    service.stop();
  }

  @Test
  public void testServerWithMultiCompDesInList() throws HiveSQLException, InterruptedException, TException {
    ThriftCLIService service = new MockServiceWithCompDes();
    service.init(serverMultiCompDes1);

    TOpenSessionReq req = new TOpenSessionReq();
    req.setConfiguration(new HashMap<String, String>());
    TOpenSessionResp resp;

    req.setConfiguration(noCompDes.getValByRegex(".*"));
    resp = service.OpenSession(req);
    assertNull(resp.getCompressorName());

    req.setConfiguration(clientSingleCompDe.getValByRegex(".*"));
    resp = service.OpenSession(req);
    assertEquals("compde3", resp.getCompressorName());
    assertEquals("1.0", resp.getCompressorVersion());

    req.setConfiguration(clientMultiCompDes1.getValByRegex(".*"));
    resp = service.OpenSession(req);
    // "compde1" fails to initialize because our mock service does not have that plug-in
    assertEquals("compde2", resp.getCompressorName());
    assertEquals("1.0", resp.getCompressorVersion());

    req.setConfiguration(clientMultiCompDes2.getValByRegex(".*"));
    resp = service.OpenSession(req);
    assertEquals("compde2", resp.getCompressorName());
    assertEquals("2.0", resp.getCompressorVersion());

    service.stop();
  }

  public class MockWithCompDeConfig extends EmbeddedThriftBinaryCLIService {
    @Override
    // Mock a plug-in with an `init` function.
    protected Map<String, String> initCompde(
        String compdeName,
        String compdeVersion,
        HiveConf serverConf,
        HiveConf clientConf) {
      if (compdeVersion.equals("1.0")) return null;
      Map<String,String> serverCompdeParams =
          getParamsForCompde(serverConf, compdeName, compdeVersion);
      Map<String,String> clientCompdeParams =
          getParamsForCompde(clientConf, compdeName, compdeVersion);
      Map<String,String> finalParams = serverCompdeParams;
      finalParams.putAll(clientCompdeParams);
      finalParams.put(noCompDeConfigPrefix("compde3-2.0") + ".test4", "compDeVal4");//overrides server
      finalParams.put(noCompDeConfigPrefix("compde3-2.0") + ".test5", "compDeVal5");//overrides client
      finalParams.put(noCompDeConfigPrefix("compde3-2.0") + ".test6", "compDeVal6");
      return finalParams;
    }
  }

  @Test
  // Ensure that the server allows the plug-in to combine the server's default
  // CompDe parameters with the client overrides and returns the final
  // configuration.
  public void testVersionParams() throws TException {
    Map<String, String> expectedConf = new HashMap<String, String>();
    expectedConf.put(noCompDeConfigPrefix("compde3") + ".version", "2.0");
    expectedConf.put(noCompDeConfigPrefix("compde3-2.0") + ".test1", "serverVal1");
    expectedConf.put(noCompDeConfigPrefix("compde3-2.0") + ".test2", "clientVal2");
    expectedConf.put(noCompDeConfigPrefix("compde3-2.0") + ".test3", "clientVal3");
    expectedConf.put(noCompDeConfigPrefix("compde3-2.0") + ".test4", "compDeVal4");
    expectedConf.put(noCompDeConfigPrefix("compde3-2.0") + ".test5", "compDeVal5");
    expectedConf.put(noCompDeConfigPrefix("compde3-2.0") + ".test6", "compDeVal6");

    ThriftCLIService service = new MockWithCompDeConfig();
    service.init(serverCompDeConf);

    TOpenSessionReq req = new TOpenSessionReq();
    req.setConfiguration(clientCompDeConf.getValByRegex(".*compressor.*"));

    TOpenSessionResp resp = service.OpenSession(req);
    assertEquals("compde3", resp.getCompressorName());
    assertEquals("2.0", resp.getCompressorVersion());
    assertEquals(expectedConf, resp.getCompressorParameters());
  }
  
  @Test
  public void testVersion() throws TException {
    
  }
}
