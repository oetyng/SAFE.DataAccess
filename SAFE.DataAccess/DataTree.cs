using System;
using System.Collections.Generic;

namespace SAFE.DataAccess
{
    public class DataTree
    {
        IMd _head;
        Action<byte[]> _onHeadAddressChange;

        public byte[] XORAddress => _head.XORAddress;

        public DataTree(IMd head, Action<byte[]> onHeadAddressChange)
        {
            _head = head;
            _onHeadAddressChange = onHeadAddressChange;
        }

        public Result<Pointer> Add(string key, Value value)
        {
            if (_head.IsFull)
            {
                // create new head, add pointer to current head in to it.
                // the level > 0 indicates its role as pointer holder
                var newHead = MdAccess.Create(_head.Level + 1);
                var pointer = new Pointer
                {
                    XORAddress = _head.XORAddress,
                    ValueType = typeof(Pointer).Name
                };
                newHead.Add(pointer);
                _head = newHead;
                _onHeadAddressChange(newHead.XORAddress);
            }

            return _head.Add(key, value);
        }

        public IEnumerable<Value> GetAllValues()
        {
            return _head.GetValues();
        }

        public IEnumerable<(Pointer, Value)> GetAllPointerValues()
        {
            return _head.GetAllPointerValues();
        }
    }
}
