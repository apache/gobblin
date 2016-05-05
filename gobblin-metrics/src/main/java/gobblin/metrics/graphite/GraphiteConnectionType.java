/* (c) 2016 Swisscom All rights reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License"); you may not use
* this file except in compliance with the License. You may obtain a copy of the
* License at  http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software distributed
* under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
* CONDITIONS OF ANY KIND, either express or implied.
*/
/*
 * Copyright (C) 2016 Swisscom All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.metrics.graphite;


import java.net.InetSocketAddress;

import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteSender;
import com.codahale.metrics.graphite.GraphiteUDP;

/**
 * Connection types used by {@link GraphiteReporter} and {@link GraphiteEventReporter}
 * 
 * @author Lorand Bendig
 *
 */
public enum GraphiteConnectionType {

  TCP {
    @Override
    public GraphiteSender createConnection(String hostname, int port) {
      return new Graphite(new InetSocketAddress(hostname, port));
    }
  }, UDP {
    @Override
    public GraphiteSender createConnection(String hostname, int port) {
      return new GraphiteUDP(new InetSocketAddress(hostname, port));
    }
  };
  
  public abstract GraphiteSender createConnection(String hostname, int port);
  
}
