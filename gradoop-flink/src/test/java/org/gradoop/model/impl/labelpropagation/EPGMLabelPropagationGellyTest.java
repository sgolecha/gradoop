package org.gradoop.model.impl.labelpropagation;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.FlinkTestBase;
import org.gradoop.model.impl.DefaultEdgeData;
import org.gradoop.model.impl.DefaultVertexData;
import org.gradoop.model.impl.operators.labelpropagation
  .EPGMLabelPropagationAlgorithm;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Tests the algorithm using the Gelly Graph.run() method.
 */
@RunWith(Parameterized.class)
public class EPGMLabelPropagationGellyTest extends FlinkTestBase {
  public EPGMLabelPropagationGellyTest(TestExecutionMode mode) {
    super(mode);
  }

  /**
   * @return a connected graph where each vertex has its id as value
   */
  static String[] getConnectedGraphWithVertexValues() {
    return new String[] {
      "0 0 1 2 3", "1 1 0 2 3", "2 2 0 1 3 4", "3 3 0 1 2", "4 4 2 5 6 7",
      "5 5 4 6 7 8", "6 6 4 5 7", "7 7 4 5 6", "8 8 5 9 10 11", "9 9 8 10 11",
      "10 10 8 9 11", "11 11 8 9 10"
    };
  }

  /**
   * All vertices are connected.
   *
   * @return a complete bipartite graph where each vertex has its id as value
   */
  static String[] getCompleteBipartiteGraphWithVertexValue() {
    return new String[] {
      "0 0 4 5 6 7", "1 1 4 5 6 7", "2 2 4 5 6 7", "3 3 4 5 6 7", "4 4 0 1 2 3",
      "5 5 0 1 2 3", "6 6 0 1 2 3", "7 7 0 1 2 3"
    };
  }

  /**
   * @return a graph containing a loop with a vertex value
   */
  static String[] getLoopGraphWithVertexValues() {
    return new String[] {"0 0 1 2", "1 1 0 3", "2 1 0 3", "3 0 1 2"};
  }

  @Test
  public void testConnectedGraphWithVertexValues() throws Exception {
    int maxIteration = 100;
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    Graph<Long, DefaultVertexData, DefaultEdgeData> epGraph =
      LabelPropagationGellyTestHelper
        .getEPGraph(getConnectedGraphWithVertexValues(), env);
    DataSet<Vertex<Long, DefaultVertexData>> labeledGraph = epGraph.run(
      new EPGMLabelPropagationAlgorithm<DefaultVertexData, DefaultEdgeData>(
        maxIteration)).getVertices();
    validateConnectedGraphResult(parseResult(labeledGraph.collect()));
  }

  @Test
  public void testBipartiteGraphWithVertexValues() throws Exception {
    int maxIteration = 100;
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    Graph<Long, DefaultVertexData, DefaultEdgeData> gellyGraph =
      LabelPropagationGellyTestHelper
        .getEPGraph(getCompleteBipartiteGraphWithVertexValue(), env);
    DataSet<Vertex<Long, DefaultVertexData>> labeledGraph = gellyGraph.run(
      new EPGMLabelPropagationAlgorithm<DefaultVertexData, DefaultEdgeData>(
        maxIteration)).getVertices();
    validateCompleteBipartiteGraphResult(parseResult(labeledGraph.collect()));
  }

  @Test
  public void testLoopGraphWithVertexValues() throws Exception {
    int maxIteration = 100;
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    Graph<Long, DefaultVertexData, DefaultEdgeData> gellyGraph =
      LabelPropagationGellyTestHelper
        .getEPGraph(getLoopGraphWithVertexValues(), env);
    DataSet<Vertex<Long, DefaultVertexData>> labeledGraph = gellyGraph.run(
      new EPGMLabelPropagationAlgorithm<DefaultVertexData, DefaultEdgeData>(
        maxIteration)).getVertices();
    validateLoopGraphResult(parseResult(labeledGraph.collect()));
  }

  private Map<Long, Long> parseResult(
    List<Vertex<Long, DefaultVertexData>> graph) {
    Map<Long, Long> result = new HashMap<>();
    for (Vertex<Long, DefaultVertexData> v : graph) {
      result.put(v.getId(), (Long) v.getValue()
        .getProperty(EPGMLabelPropagationAlgorithm.CURRENT_VALUE));
    }
    return result;
  }

  private void validateConnectedGraphResult(Map<Long, Long> vertexIDWithValue) {
    assertEquals(12, vertexIDWithValue.size());
    assertEquals(0, vertexIDWithValue.get(0L).longValue());
    assertEquals(0, vertexIDWithValue.get(1L).longValue());
    assertEquals(0, vertexIDWithValue.get(2L).longValue());
    assertEquals(0, vertexIDWithValue.get(3L).longValue());
    assertEquals(4, vertexIDWithValue.get(4L).longValue());
    assertEquals(4, vertexIDWithValue.get(5L).longValue());
    assertEquals(4, vertexIDWithValue.get(6L).longValue());
    assertEquals(4, vertexIDWithValue.get(7L).longValue());
    assertEquals(8, vertexIDWithValue.get(8L).longValue());
    assertEquals(8, vertexIDWithValue.get(9L).longValue());
    assertEquals(8, vertexIDWithValue.get(10L).longValue());
    assertEquals(8, vertexIDWithValue.get(11L).longValue());
  }

  private void validateCompleteBipartiteGraphResult(
    Map<Long, Long> vertexIDWithValue) {
    assertEquals(8, vertexIDWithValue.size());
    assertEquals(0, vertexIDWithValue.get(0L).longValue());
    assertEquals(0, vertexIDWithValue.get(1L).longValue());
    assertEquals(0, vertexIDWithValue.get(2L).longValue());
    assertEquals(0, vertexIDWithValue.get(3L).longValue());
    assertEquals(0, vertexIDWithValue.get(4L).longValue());
    assertEquals(0, vertexIDWithValue.get(5L).longValue());
    assertEquals(0, vertexIDWithValue.get(6L).longValue());
    assertEquals(0, vertexIDWithValue.get(7L).longValue());
  }

  private void validateLoopGraphResult(Map<Long, Long> vertexIDWithValue) {
    assertEquals(4, vertexIDWithValue.size());
    assertEquals(0, vertexIDWithValue.get(0L).longValue());
    assertEquals(0, vertexIDWithValue.get(1L).longValue());
    assertEquals(0, vertexIDWithValue.get(2L).longValue());
    assertEquals(0, vertexIDWithValue.get(3L).longValue());
  }
}
