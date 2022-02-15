using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;

namespace Sir.VectorSpace
{
    public static class GraphBuilder
    {
        public static VectorNode CreateTree<T>(this IModel<T> model, IIndexingStrategy indexingStrategy, params T[] data)
        {
            var root = new VectorNode();

            foreach (var item in data)
            {
                foreach (var vector in model.CreateEmbedding(item, true))
                {
                    indexingStrategy.ExecutePut<T>(root, new VectorNode(vector));
                }
            }

            return root;
        }

        public static void AddOrAppendSupervised(
            this VectorNode root,
            VectorNode node,
            IModel model)
        {
            var cursor = root;

            while (true)
            {
                var angle = cursor.Vector == null ? 0 : model.CosAngle(node.Vector, cursor.Vector);

                if (angle >= model.IdenticalAngle)
                {
                    if (!cursor.Vector.Label.Equals(node.Vector.Label))
                        throw new InvalidOperationException($"IdenticalAngle {model.IdenticalAngle} is too low. Angle was {angle}");

                    MergeDocIds(cursor, node);
                    break;
                }
                else if (angle > model.FoldAngle)
                {
                    if (cursor.Left == null)
                    {
                        cursor.Left = node;
                        break;
                    }
                    else
                    {
                        cursor = cursor.Left;
                    }
                }
                else
                {
                    if (cursor.Right == null)
                    {
                        cursor.Right = node;
                        break;
                    }
                    else
                    {
                        cursor = cursor.Right;
                    }
                }
            }
        }

        public static void AddOrAppend(
            this VectorNode root, 
            VectorNode node,
            IModel model)
        {
            var cursor = root;

            while (true)
            {
                var angle = cursor.Vector == null ? 0 : model.CosAngle(node.Vector, cursor.Vector);

                if (angle >= model.IdenticalAngle)
                {
                    MergeDocIds(cursor, node);

                    break;
                }
                else if (angle > model.FoldAngle)
                {
                    if (cursor.Left == null)
                    {
                        cursor.Left = node;
                        break;
                    }
                    else
                    {
                        cursor = cursor.Left;
                    }
                }
                else
                {
                    if (cursor.Right == null)
                    {
                        cursor.Right = node;
                        break;
                    }
                    else
                    {
                        cursor = cursor.Right;
                    }
                }
            }
        }

        public static void AddIfUnique(
            this VectorNode root,
            VectorNode node,
            IModel model)
        {
            var cursor = root;

            while (true)
            {
                var angle = cursor.Vector == null ? 0 : model.CosAngle(node.Vector, cursor.Vector);

                if (angle >= model.IdenticalAngle)
                {
                    break;
                }
                else if (angle > model.FoldAngle)
                {
                    if (cursor.Left == null)
                    {
                        cursor.Left = node;
                        break;
                    }
                    else
                    {
                        cursor = cursor.Left;
                    }
                }
                else
                {
                    if (cursor.Right == null)
                    {
                        cursor.Right = node;
                        break;
                    }
                    else
                    {
                        cursor = cursor.Right;
                    }
                }
            }
        }

        public static bool TryAdd(
            this VectorNode root,
            VectorNode node,
            IModel model)
        {
            var cursor = root;

            while (true)
            {
                var angle = cursor.Vector == null ? 0 : model.CosAngle(node.Vector, cursor.Vector);

                if (angle >= model.IdenticalAngle)
                {
                    return false;
                }
                else if (angle > model.FoldAngle)
                {
                    if (cursor.Left == null)
                    {
                        cursor.Left = node;

                        return true;
                    }
                    else
                    {
                        cursor = cursor.Left;
                    }
                }
                else
                {
                    if (cursor.Right == null)
                    {
                        cursor.Right = node;

                        return true;
                    }
                    else
                    {
                        cursor = cursor.Right;
                    }
                }
            }
        }

        public static void Build(
            this VectorNode root,
            VectorNode node,
            IModel model)
        {
            var cursor = root;

            while (true)
            {
                var angle = cursor.Vector == null ? 0 : model.CosAngle(node.Vector, cursor.Vector);

                if (angle >= model.IdenticalAngle)
                {
                    break;
                }
                else if (angle > model.FoldAngle)
                {
                    if (cursor.Left == null)
                    {
                        cursor.Left = node;
                        break;
                    }
                    else
                    {
                        cursor = cursor.Left;
                    }
                }
                else
                {
                    if (cursor.Right == null)
                    {
                        cursor.Right = node;
                        break;
                    }
                    else
                    {
                        cursor = cursor.Right;
                    }
                }
            }
        }

        public static void MergeDocIds(this VectorNode target, VectorNode source)
        {
            target.DocIds.AddRange(source.DocIds);
        }



        public static void Serialize(this VectorNode node, Stream stream)
        {
            long terminator;

            if (node.Left == null && node.Right == null) // there are no children
            {
                terminator = 3;
            }
            else if (node.Left == null) // there is a right but no left
            {
                terminator = 2;
            }
            else if (node.Right == null) // there is a left but no right
            {
                terminator = 1;
            }
            else // there is a left and a right
            {
                terminator = 0;
            }

            stream.Write(BitConverter.GetBytes(node.VectorOffset), 0, sizeof(long));
            stream.Write(BitConverter.GetBytes(node.PostingsOffset), 0, sizeof(long));
            stream.Write(BitConverter.GetBytes((long)node.Vector.ComponentCount), 0, sizeof(long));
            stream.Write(BitConverter.GetBytes(node.Weight), 0, sizeof(long));
            stream.Write(BitConverter.GetBytes(terminator), 0, sizeof(long));
        }

        /// <summary>
        /// Persist tree to disk.
        /// </summary>
        /// <param name="node">Tree to persist.</param>
        /// <param name="indexStream">stream to persist tree into</param>
        /// <param name="vectorStream">stream to persist vectors in</param>
        /// <param name="postingsStream">optional stream to persist any posting references into</param>
        /// <returns></returns>
        public static (long offset, long length) SerializeTree(this VectorNode node, Stream indexStream, Stream vectorStream, Stream postingsStream = null)
        {
            var stack = new Stack<VectorNode>();
            var offset = indexStream.Position;
            var length = 0;

            if (node.ComponentCount == 0)
            {
                node = node.Right;
            }

            while (node != null)
            {
                if (node.PostingsOffset == -1 && postingsStream != null)
                    SerializePostings(node, postingsStream);

                node.VectorOffset = SerializableVector.Serialize(node.Vector, vectorStream);

                Serialize(node, indexStream);

                length += VectorNode.BlockSize;

                if (node.Right != null)
                {
                    stack.Push(node.Right);
                }

                node = node.Left;

                if (node == null && stack.Count > 0)
                {
                    node = stack.Pop();
                }
            }

            return (offset, length);
        }

        public static void SerializePostings(this VectorNode node, Stream postingsStream)
        {
            node.PostingsOffset = postingsStream.Position;

            SerializeHeaderAndPostingsPayload(node.DocIds, node.DocIds.Count, postingsStream);
        }

        public static void SerializeHeaderAndPostingsPayload(IList<long> items, int itemCount, Stream stream)
        {
            if (items.Count == 0) throw new ArgumentException("can't be empty", nameof(items));

            stream.Write(BitConverter.GetBytes((long)itemCount));

            foreach (var item in items)
            {
                stream.Write(BitConverter.GetBytes(item));
            }
        }
    }
}