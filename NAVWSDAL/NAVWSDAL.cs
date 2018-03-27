using System;
using System.Data;

namespace NAVWSDAL
{
    public class NAVWSDAL
    {
        public void CallNAVWS(DataTable data)
        {
            foreach (DataRow row in data.Rows)
            {
                //Read the Order fields and call a NAV WS codeunit
            }
        }
    }
}
