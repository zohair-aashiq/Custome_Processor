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
package org.apache.nifi.processors.saiftylake;
package de.coac.robots.saifty.lake

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class MyProcessor extends AbstractProcessor {

    public static final PropertyDescriptor JDBC_PROPERTY = new PropertyDescriptor
            .Builder().name("JDBC connection")
            .displayName("JDBC connection for hive")
            .description("JDBC connection for hive")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("localhost")
            .build();

    public static final PropertyDescriptor PHRASE_PROPERTY = new PropertyDescriptor
            .Builder().name("PHRASE")
            .displayName("PHRASE Integration")
            .description("Integrate the data phrase core ")
            .required(true)
            .allowableValues("true","false")
            .defaultValue("true")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor PORT_PROPERTY = new PropertyDescriptor
            .Builder().name("PORT")
            .displayName("PORT NUMBER")
            .description("PORT NUMBER")
            .required(true)
            .defaultValue("1000")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor USAGE_PROPERTY = new PropertyDescriptor
            .Builder().name("USAGE")
            .displayName("USAGE Integration")
            .description("Integrate the data USAGE core")
            .required(true)
            .allowableValues("true","false")
            .defaultValue("true")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor COMPOSITIONS_PROPERTY = new PropertyDescriptor
            .Builder().name("COMPOSITIONS")
            .displayName("COMPOSITIONS Integration")
            .description("Integrate the data COMPOSITIONS core ")
            .required(true)
            .allowableValues("true","false")
            .defaultValue("true")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor PRODUCTS_PROPERTY = new PropertyDescriptor
            .Builder().name("PRODUCTS")
            .displayName("PRODUCTS Integration")
            .description("Integrate the data PRODUCTS core ")
            .required(true)
            .allowableValues("true","false")
            .defaultValue("true")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor PROPERTIES_PROPERTY = new PropertyDescriptor
            .Builder().name("PROPERTIES")
            .displayName("PROPERTIES Integration")
            .description("Integrate the data PROPERTIES core ")
            .required(true)
            .allowableValues("true","false")
            .defaultValue("true")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor ESTDH_PROPERTY = new PropertyDescriptor
            .Builder().name("ESTDH")
            .displayName("ESTDH Integration")
            .description("Integrate the data ESTDH core ")
            .required(true)
            .allowableValues("true","false")
            .defaultValue("true")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor ESTMJ_PROPERTY = new PropertyDescriptor
            .Builder().name("ESTMJ")
            .displayName("ESTMJ Integration")
            .description("Integrate the data ESTMJ core ")
            .required(true)
            .allowableValues("true","false")
            .defaultValue("true")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Operation completed successfully")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("FAILURE")
            .description("Operation failed")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = new ArrayList<>();
        descriptors.add(JDBC_PROPERTY);
        descriptors.add(PHRASE_PROPERTY);
        descriptors.add(USAGE_PROPERTY);
        descriptors.add(COMPOSITIONS_PROPERTY);
        descriptors.add(PRODUCTS_PROPERTY);
        descriptors.add(PROPERTIES_PROPERTY);
        descriptors.add(ESTDH_PROPERTY);
        descriptors.add(ESTMJ_PROPERTY);
        descriptors = Collections.unmodifiableList(descriptors);

        relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        String urlJDBC = context.getProperty(JDBC_PROPERTY).getValue();
        Boolean phrases = context.getProperty(PHRASE_PROPERTY).asBoolean();
        Boolean usage = context.getProperty(USAGE_PROPERTY).asBoolean();
        Boolean compositions = context.getProperty(COMPOSITIONS_PROPERTY).asBoolean();
        Boolean products = context.getProperty(PRODUCTS_PROPERTY).asBoolean();
        Boolean properties = context.getProperty(PROPERTIES_PROPERTY).asBoolean();
        Boolean estdh = context.getProperty(ESTDH_PROPERTY).asBoolean();
        Boolean estmj = context.getProperty(ESTMJ_PROPERTY).asBoolean();

    }
}
