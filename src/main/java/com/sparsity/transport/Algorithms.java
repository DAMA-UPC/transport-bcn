package com.sparsity.transport;

import com.sparsity.sparksee.algorithms.SinglePairShortestPathBFS;
import com.sparsity.sparksee.gdb.*;

import java.util.ArrayList;

public class Algorithms {

    public static ArrayList<String> findRoute(Session session, Schema schema, String stopA, String stopB) {

        Graph graph = session.getGraph();

        Objects aux = graph.select(schema.getStopIdType(), Condition.Equal, (new Value()).setString(stopA));
        long stopAOid = aux.any();
        aux.close();
        aux = graph.select(schema.getStopIdType(), Condition.Equal, (new Value()).setString(stopB));
        long stopBOid = aux.any();
        aux.close();

        SinglePairShortestPathBFS shortestPath = new SinglePairShortestPathBFS(session,
                                                                               stopAOid,
                                                                               stopBOid);

        shortestPath.addNodeType(schema.getStopType());
        shortestPath.addEdgeType(schema.getConnectsType(), EdgesDirection.Any);
        shortestPath.run();

        OIDList list = shortestPath.getPathAsNodes();
        ArrayList<String> path = new ArrayList<String>();
        for( long stop : list) {
            Value value = new Value();
            graph.getAttribute(stop, schema.getStopIdType(), value);
            path.add(value.getString());
        }

        return path;
    }
}
