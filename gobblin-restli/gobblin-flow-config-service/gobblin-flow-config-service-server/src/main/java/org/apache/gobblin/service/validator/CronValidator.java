/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.service.validator;

import org.quartz.CronExpression;

import com.linkedin.data.DataMap;
import com.linkedin.data.element.DataElement;
import com.linkedin.data.message.Message;
import com.linkedin.data.schema.validator.AbstractValidator;
import com.linkedin.data.schema.validator.ValidatorContext;


/**
 * Validates the String value to ensure it is a valid Cron expression
 */
public class CronValidator extends AbstractValidator
{
  public CronValidator(DataMap config)
  {
    super(config);
  }

  @Override
  public void validate(ValidatorContext ctx)
  {
    DataElement element = ctx.dataElement();
    Object value = element.getValue();
    String str = String.valueOf(value);

    if (!CronExpression.isValidExpression(str))
    {
      ctx.addResult(new Message(element.path(), "\"%1$s\" is not in Cron format", str));
    }
  }
}