/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.test.functional.util;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;

public class MetricsClient {

  private static final Logger log = LoggerFactory.getLogger(MetricsClient.class);

  public static class MetricsResp {
    private final int code;
    private final Metrics2TestSink.JsonValues v;

    public MetricsResp(final int code, Metrics2TestSink.JsonValues v){
      this.code = code;
      this.v = v;
    }
    @Override public String toString() {
      final StringBuilder sb = new StringBuilder("MetricsResp{");
      sb.append("code=").append(code);
      sb.append(", v=").append(v);
      sb.append('}');
      return sb.toString();
    }
  }

  public MetricsResp getMetrics() throws Exception {

    CloseableHttpClient httpclient = HttpClients.createDefault();

    final String u = String.format("http://%s:%d/metrics", InetAddress.getLocalHost().getHostAddress(),12332);

    log.info("Connect to: {}", u);

    try {

      HttpGet httpget = new HttpGet(u);

      log.info("Executing request {}", httpget.getRequestLine());

      // Create a custom response handler
      ResponseHandler<MetricsResp> responseHandler = new ResponseHandler<MetricsResp>() {

        @Override
        public MetricsResp handleResponse(
            final HttpResponse response) throws ClientProtocolException, IOException {

          int status = response.getStatusLine().getStatusCode();

          log.error("response ---> {}", status);

          if(status == 204){
            MetricsResp answer = new MetricsResp(status, null);
            return answer;
          }

          if (status >= 200 && status < 300) {

            HttpEntity entity = response.getEntity();
            if(entity != null){
              return new MetricsResp(status,
                  Metrics2TestSink.JsonValues.fromJson(IOUtils.toString(entity.getContent())));
            }

            return new MetricsResp(status, null);

          } if(status == 404 || status >= 500) {
            log.info("Could not connect to metrics server at {}, received {}",u,status);
            return new MetricsResp(status, null);
          } else {
            throw new ClientProtocolException("Unexpected response status: " + status);
          }
        }

      };

      try {
        MetricsResp values = httpclient.execute(httpget, responseHandler);
        log.info("RESP: {}", values);
        return values;
      }catch(Exception ex){
        log.debug("Could not connect to metrics server at {}", u);
        return new MetricsResp(-1, null);
      }


    } finally {
      httpclient.close();
    }
  }
}
