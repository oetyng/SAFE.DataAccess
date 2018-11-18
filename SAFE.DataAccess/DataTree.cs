using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace SAFE.DataAccess
{
    public class DataTree
    {
        IMd _head;
        Func<byte[], Task> _onHeadAddressChange;

        public byte[] XORAddress => _head.XORAddress;

        public DataTree(IMd head, Func<byte[], Task> onHeadAddressChange)
        {
            _head = head;
            _onHeadAddressChange = onHeadAddressChange;
        }

        public async Task<Result<Pointer>> AddAsync(string key, Value value)
        {
            if (_head.IsFull)
            {
                // create new head, add pointer to current head in to it.
                // the level > 0 indicates its role as pointer holder
                var newHead = await MdAccess.CreateAsync(_head.Level + 1).ConfigureAwait(false);
                var pointer = new Pointer
                {
                    XORAddress = _head.XORAddress,
                    ValueType = typeof(Pointer).Name
                };
                await newHead.AddAsync(pointer).ConfigureAwait(false);
                _head = newHead;
                await _onHeadAddressChange(newHead.XORAddress).ConfigureAwait(false);
            }

            return await _head.AddAsync(key, value).ConfigureAwait(false);
        }

        public Task<IEnumerable<Value>> GetAllValuesAsync()
        {
            return _head.GetAllValuesAsync();
        }

        public Task<IEnumerable<(Pointer, Value)>> GetAllPointerValuesAsync()
        {
            return _head.GetAllPointerValuesAsync();
        }
    }
}
