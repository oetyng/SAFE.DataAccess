
namespace SAFE.DataAccess
{
    public class MdHead
    {
        public MdHead(IMd md, string id)
        {
            Md = md;
            Id = id;
        }

        public IMd Md { get; }
        public string Id { get; }
    }
}
