﻿using System.Collections.Generic;
using System.Text;

namespace Sir.Store
{
    public static class PathFinder
    {
        public static Hit ClosestMatch(VectorNode root, Vector vector, IStringModel model)
        {
            var best = root;
            var cursor = root;
            float highscore = 0;

            while (cursor != null)
            {
                var angle = model.CosAngle(vector, cursor.Vector);

                if (angle > model.FoldAngle)
                {
                    if (angle > highscore)
                    {
                        highscore = angle;
                        best = cursor;
                    }

                    cursor = cursor.Left;
                }
                else
                {
                    if (angle > highscore)
                    {
                        highscore = angle;
                        best = cursor;
                    }
                    cursor = cursor.Right;
                }
            }

            return new Hit
            {
                Score = highscore,
                Node = best
            };
        }

        public static IEnumerable<VectorNode> All(VectorNode root)
        {
            var node = root.ComponentCount == 0 ? root.Right : root;
            var stack = new Stack<VectorNode>();

            while (node != null)
            {
                yield return node;

                if (node.Right != null)
                {
                    stack.Push(node.Right);
                }

                node = node.Left;

                if (node == null)
                {
                    if (stack.Count > 0)
                        node = stack.Pop();
                }
            }
        }

        public static string Visualize(VectorNode root)
        {
            StringBuilder output = new StringBuilder();
            Visualize(root, output, 0);
            return output.ToString();
        }

        private static void Visualize(VectorNode node, StringBuilder output, int depth)
        {
            if (node == null) return;

            output.Append('\t', depth);
            output.AppendFormat($"{node.AngleWhenAdded} {node} w:{node.Weight}");
            output.AppendLine();

            Visualize(node.Left, output, depth + 1);
            Visualize(node.Right, output, depth);
        }

        public static (int depth, int width, int avgDepth) Size(VectorNode root)
        {
            var width = 0;
            var depth = 1;
            var node = root;
            var aggDepth = 0;
            var count = 0;

            while (node != null)
            {
                var d = Depth(node);
                if (d > depth)
                {
                    depth = d;
                }

                aggDepth += d;
                count++;
                width++;

                node = node.Right;
            }

            return (depth, width, aggDepth / count);
        }

        public static int Depth(VectorNode node)
        {
            var count = 0;

            while (node != null)
            {
                count++;
                node = node.Left;
            }
            return count;
        }
    }
}
