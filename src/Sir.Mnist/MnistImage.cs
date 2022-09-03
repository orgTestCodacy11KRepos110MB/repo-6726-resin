using Sir.Images;
using System;
using System.Text;

namespace Sir.Mnist
{
    /// <summary>
    /// Pixel values are 0 to 255. 0 means background(white), 255 means foreground(black). 
    /// </summary>
    public class MnistImage : IImage
    {
        public byte[] Pixels { get; private set; }

        public string Label { get; }

        public MnistImage(byte[] pixels, byte label)
        {
            Pixels = pixels;
            Label = label.ToString();
        }

        public string Print()
        {
            var s = new StringBuilder();
            var rows = new byte[28][];
            
            for (int i = 0;i < rows.Length; i++)
            {
                var offset = i * 28;
                var row = new byte[28];

                Buffer.BlockCopy(Pixels, offset, row, 0, row.Length);

                rows[i] = row;
            }

            for (int i = 0; i < rows.Length; ++i)
            {
                for (int j = 0; j < rows[i].Length; ++j)
                {
                    if (rows[i][j] == 0)
                        s.Append(' '); // white

                    else if (rows[i][j] >= 250)
                        s.Append('O'); // black-ish

                    else
                        s.Append('.'); // gray
                }
                s.Append('\n');
            }

            return s.ToString();
        }

        public override string ToString()
        {
            return Label?.ToString();
        }

        public byte[] GetBytes()
        {
            return Pixels;
        }
    }
}
