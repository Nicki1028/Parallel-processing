using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace 平行處理
{
     public static class MedianExtention
     {
       
            public static double Median(this IEnumerable<double> source)
            {
                if (source is null || !source.Any())
                {
                    throw new InvalidOperationException("Cannot compute median for a null or empty set.");
                }

                var sortedList =
                    source.OrderBy(number => number).ToList();

                int itemIndex = sortedList.Count / 2;

                if (sortedList.Count % 2 == 0)
                {
                    // Even number of items.
                    return (sortedList[itemIndex] + sortedList[itemIndex - 1]) / 2;
                }
                else
                {
                    // Odd number of items.
                    return sortedList[itemIndex];
                }
            }
        
    }
}
