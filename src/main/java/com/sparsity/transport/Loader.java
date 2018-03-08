package com.sparsity.transport;

import com.sparsity.sparksee.gdb.*;
import com.sparsity.sparksee.script.ScriptParser;

import java.util.ArrayList;

public class Loader {

    public static void main(String[] args) throws Exception {

        ScriptParser scriptParser = new ScriptParser();
        scriptParser.parse("transport.schema",true,"");
        scriptParser.parse("transport.load",true,"");

        Sparksee sparksee = new Sparksee(new SparkseeConfig());
        Database database = sparksee.open("transport.gdb",true);
        Session session = database.newSession();

        Schema schema = new Schema(session.getGraph());

        ArrayList<String> route = Algorithms.findRoute(session, schema, "1.111","1.140" );

        Graph graph = session.getGraph();
        for( String s : route ) {
            Objects objects = graph.select(schema.getStopIdType(), Condition.Equal, (new Value()).setString(s));
            Value value = new Value();
            graph.getAttribute(objects.any(), schema.getStopNameType(),value);
            System.out.println(value.getString());
            objects.close();
        }

        session.close();
        database.close();
        sparksee.close();
    }

}
