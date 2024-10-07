/*
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

package org.apache.cassandra.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MutableIntervalTree<C extends Comparable<? super C>, D, I extends Interval<C, D>> implements Iterable<I>
{
    private static final Logger logger = LoggerFactory.getLogger(MutableIntervalTree.class);

    private final MutableIntervalNode head;
    private final int count;

    protected MutableIntervalTree(Collection<I> intervals)
    {
        this.head = intervals == null || intervals.isEmpty() ? null : new fromIntervals(intervals);
        this.count = intervals == null ? 0 : intervals.size();
    }

    @Override
    public Iterator<I> iterator()
    {
        // TODO
        return null;
    }

    @Override
    public void forEach(Consumer<? super I> action)
    {
        // TODO
        Iterable.super.forEach(action);
    }

    @Override
    public Spliterator<I> spliterator()
    {
        // TODO
        return Iterable.super.spliterator();
    }

    private class MutableIntervalNode
    {
        private int depth;
        private int balance;
        private C center;

        private Set<I> intersect;
        private MutableIntervalNode left;
        private MutableIntervalNode right;

        public MutableIntervalNode(Set<I> intersect, C center, MutableIntervalNode left, MutableIntervalNode right)
        {
            this.intersect = intersect;
            this.center = center;
            this.left = left;
            this.right = right;
            this.depth = 0;
            this.balance = 0;
            // depth and balance will be set
            this.rotate();
        }

        public MutableIntervalNode fromInterval(I interval)
        {
            return new MutableIntervalNode(new HashSet<>(), interval.min, null, null);
        }

        public MutableIntervalNode fromIntervals(Collection<I> intervals)
        {
            if (intervals == null)
            {
                return null;
            }
            List<I> sortedIntervals = new ArrayList<>(intervals);
            // Compare by min then max
            sortedIntervals.sort(Comparator.comparing((I o) -> o.min).thenComparing(o -> o.max));
            return fromSortedIntervals(sortedIntervals);
        }

        public MutableIntervalNode fromSortedIntervals(List<I> sortedIntervals)
        {
            if (sortedIntervals == null)
            {
                return null;
            }
            MutableIntervalNode node = new MutableIntervalNode(new HashSet<>(), null, null, null);
            I centerInterval = sortedIntervals.get(sortedIntervals.size() / 2);
            node.center = centerInterval.min;
            List<I> sortedLeftIntervals = new ArrayList<>();
            List<I> sortedRightIntervals = new ArrayList<>();
            for (I interval : sortedIntervals)
            {
                if (node.center.compareTo(interval.max) >= 0)
                {
                    sortedLeftIntervals.add(interval);
                }
                else if (node.center.compareTo(interval.min) < 0)
                {
                    sortedRightIntervals.add(interval);
                }
                else
                {
                    node.intersect.add(interval);
                }
            }
            node.left = fromSortedIntervals(sortedLeftIntervals);
            node.right = fromSortedIntervals(sortedRightIntervals);
            return node.rotate();
        }

        public boolean isOverlapWithCenter(I interval)
        {
            return interval.min.compareTo(this.center) <= 0 && interval.max.compareTo(this.center) > 0;
        }

        public boolean isInRightBranch(I interval)
        {
            return interval.min.compareTo(this.center) > 0;
        }

        public void refreshBalance()
        {
            int leftDepth = this.left == null ? 0 : this.left.depth;
            int rightDepth = this.right == null ? 0 : this.right.depth;
            if (leftDepth >= rightDepth)
            {
                this.depth = leftDepth + 1;
            }
            else
            {
                this.depth = rightDepth + 1;
            }
            this.balance = rightDepth - leftDepth;
        }

        @VisibleForTesting
        public int computeDepth()
        {
            int leftDepth = this.left == null ? 0 : this.left.depth;
            int rightDepth = this.right == null ? 0 : this.right.depth;
            return leftDepth >= rightDepth ? leftDepth + 1 : rightDepth + 1;
        }

        private MutableIntervalNode rotate()
        {
            this.refreshBalance();
            if (Math.abs(this.balance) >= 2)
            {
                boolean myHeavy = this.isRightSideHeavy();
                boolean childHeavy = getChild(myHeavy).isRightSideHeavy();
                // both my heavy side and the heavy side child are heavy on the same side -> single rotate
                if (myHeavy == childHeavy || getChild(myHeavy).balance == 0)
                {
                    return srotate();
                }
                else
                {
                    return drotate();
                }
            }
            return this;
        }

        private MutableIntervalNode srotate()
        {
            boolean myHeavy = this.isRightSideHeavy();
            boolean myLight = !myHeavy;
            MutableIntervalNode newHead = this.getChild(myHeavy);
            this.setChild(myHeavy, newHead.getChild(myLight));
            newHead.setChild(myLight, this.rotate());

            List<I> promotees = new ArrayList<>();
            for (I interval : newHead.getChild(myLight).intersect)
            {
                if (newHead.isOverlapWithCenter(interval))
                {
                    promotees.add(interval);
                }
            }
            if (!promotees.isEmpty())
            {
                for (I interval : promotees)
                {
                    newHead.setChild(myLight, newHead.getChild(myLight).remove(interval));
                }
            }

            newHead.refreshBalance();
            return newHead;
        }

        private MutableIntervalNode drotate()
        {
            boolean myHeavy = this.isRightSideHeavy();
            this.setChild(myHeavy, this.getChild(myHeavy).srotate());
            this.refreshBalance();
            return this.srotate();
        }

        public MutableIntervalNode add(I interval)
        {
            if (this.isOverlapWithCenter(interval))
            {
                this.intersect.add(interval);
                return this;
            }
            else
            {
                boolean direction = this.isInRightBranch(interval);
                if (this.getChild(direction) == null)
                {
                    this.setChild(direction, this.fromInterval(interval));
                    this.refreshBalance();
                    return this;
                }
                else
                {
                    this.setChild(direction, this.getChild(direction).add(interval));
                    return this.rotate();
                }
            }
        }

        public boolean isRightSideHeavy()
        {
            return this.balance > 0;
        }

        public MutableIntervalNode getChild(boolean getRightChild)
        {
            return getRightChild ? this.right : this.left;
        }

        public void setChild(boolean isSetRightChild, MutableIntervalNode node)
        {
            if (isSetRightChild)
            {
                this.right = node;
            }
            else
            {
                this.left = node;
            }
        }

        public MutableIntervalNode remove(I interval)
        {
            List<Integer> done = new ArrayList<>();
            return this.removeIntervalHelper(interval, done);
        }

        public MutableIntervalNode removeIntervalHelper(I interval, List<Integer> done)
        {
            if (this.isOverlapWithCenter(interval))
            {
                this.intersect.remove(interval);
                if (!this.intersect.isEmpty())
                {
                    done.add(1);
                    return this;
                }
                else
                {
                    return this.prune();
                }
            }
            else
            {
                boolean direction = this.isInRightBranch(interval);
                if (this.getChild(direction) == null)
                {
                    // should not hit this
                    done.add(1);
                    return this;
                }
                this.setChild(direction, this.getChild(direction).removeIntervalHelper(interval, done));

                if (!done.isEmpty())
                {
                    return this.rotate();
                }
                return this;
            }
        }

        public MutableIntervalNode prune()
        {
            if (this.getChild(true) == null || this.getChild(false) == null)
            {
                return this.getChild(true) != null ? this.getChild(true) : this.getChild(false);
            }
            else
            {
                Pair<MutableIntervalNode, MutableIntervalNode> p = this.getChild(false).popGreatestChild();
                this.setChild(false, p.right);
                MutableIntervalNode newNode = p.left;
                newNode.setChild(false, this.left);
                newNode.setChild(true, this.right);
                newNode.refreshBalance();
                newNode = newNode.rotate();
                return newNode;
            }
        }

        private Pair<MutableIntervalNode, MutableIntervalNode> popGreatestChild()
        {
            if (this.right != null)
            {
                List<I> maxOrderIntervals = new ArrayList<>(this.intersect);
                // Compare by max then min
                maxOrderIntervals.sort(Comparator.comparing((I o) -> o.max).thenComparing(o -> o.min));
                // pop the max
                I maxInterval = maxOrderIntervals.get(0);
                maxOrderIntervals.remove(0);
                C newCenter = this.center;
                while (!maxOrderIntervals.isEmpty())
                {
                    // pop the max
                    I nextMaxInterval = maxOrderIntervals.get(0);
                    maxOrderIntervals.remove(0);
                    if (nextMaxInterval.max == maxInterval.max)
                    {
                        continue;
                    }
                    if (newCenter.compareTo(maxInterval.max) < 0)
                    {
                        newCenter = maxInterval.max;
                    }
                }
                // this node doesn't have children set yet
                MutableIntervalNode child = new MutableIntervalNode(getNewIntersect(newCenter), newCenter, null, null);
                this.intersect.removeAll(child.intersect);

                if (!this.intersect.isEmpty())
                {
                    return Pair.create(child, this);
                }
                else
                {
                    return Pair.create(child, this.getChild(false)); // rotate left child
                }
            }
            else
            {
                Pair<MutableIntervalNode, MutableIntervalNode> p = this.getChild(true).popGreatestChild();
                MutableIntervalNode greatestChild = p.left;
                this.setChild(false, p.right);

                for (I interval : this.intersect)
                {
                    if (interval.min.compareTo(greatestChild.center) <= 0 && interval.max.compareTo(greatestChild.center) > 0)
                    {
                        this.intersect.remove(interval);
                        greatestChild.add(interval);
                    }
                }

                if (!this.intersect.isEmpty())
                {
                    this.refreshBalance();
                    MutableIntervalNode newHead = this.rotate();
                    return Pair.create(greatestChild, newHead);
                }
                else
                {
                    MutableIntervalNode newHead = this.prune();
                    return Pair.create(greatestChild, newHead);
                }
            }
        }

        private Set<I> getNewIntersect(C newCenter)
        {
            Set<I> res = new HashSet<>();
            for (I interval : this.intersect)
            {
                if (interval.min.compareTo(newCenter) <= 0 && interval.max.compareTo(newCenter) > 0)
                {
                    res.add(interval);
                }
            }
            return res;
        }

        @Override
        public String toString()
        {
            return String.format("MutableIntervalNode<Center=%s, depth=%d, balance=%d>",
                                 center.toString(), depth, balance);
        }
    }
}
