using System;
using System.IO;
using System.Collections.Generic;
using System.Collections;
using System.Linq;
using System.Text;
using DotNetSiemensPLCToolBoxLibrary.DBF.Enums;
using DotNetSiemensPLCToolBoxLibrary.DBF.Structures;
using DotNetSiemensPLCToolBoxLibrary.DBF.Structures.DBT;
using DotNetSiemensPLCToolBoxLibrary.General;

namespace DotNetSiemensPLCToolBoxLibrary.DBF
{
    class DBFFile : IDisposable 
    {
        private readonly Stream inputDBFStream;
        private string fileName;
        private string fullPath;
        private bool isReadOnlyFile = true;
        private DBFHeader dbfFileHeader;
        private ArrayList dbfFilefields;

        public DBFFile() : this(null)
        {
        }

        public DBFFile(Stream inputDBFStream)
        {
            this.inputDBFStream = inputDBFStream;
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {

        }

    }
}
